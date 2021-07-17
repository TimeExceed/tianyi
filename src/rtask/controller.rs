use avro_rs::types as avro;
use log::*;
use static_init::dynamic;
use std::collections::BTreeMap;
use std::convert::TryInto;
use std::path::Path;
use super::*;
use tokio::sync::mpsc;
use tokio::sync::watch;

#[derive(Clone)]
pub struct Controller {
    progress_watcher: watch::Receiver<Progress>,
}

pub type EventWatcher = tokio::sync::watch::Receiver<Progress>;

const BUCKET_NAME: &str = "RTASK";

impl Controller {
    pub fn new<R>(
        filename: &Path, 
        recovery: R,
    ) -> Result<Self, Error> 
    where R: Factory + Send + 'static {
        let (sender, watcher) = watch::channel(Progress::default());
        let ctrl = Controller{
            progress_watcher: watcher,
        };
        let store_cfg = kv::Config::new(filename)
            .temporary(true);
        let store = kv::Store::new(store_cfg)?;
        tokio::spawn(async move {
            start(store, recovery, sender).await;
        });
        Ok(ctrl)
    }

    pub fn subscribe_progress(&self) -> EventWatcher {
        self.progress_watcher.clone()
    }

    pub async fn wait_until_completion(self) -> Option<Progress> {
        let mut watcher = self.progress_watcher.clone();
        let mut latest_progress = None;
        while watcher.changed().await.is_ok() {
            latest_progress = Some(watcher.borrow().clone());
        }
        latest_progress
    }
}

async fn start<R>(
    db: kv::Store,
    recovery: R,
    progress_sender: watch::Sender<Progress>,
) where R: Factory + Send + 'static {
    let factory_name = std::any::type_name::<R>();
    let bucket = db.bucket::<kv::Raw, kv::Raw>(Some(BUCKET_NAME)).unwrap();
    let (mut progress, entry_tasks) = if bucket.is_empty() {
        info!("Init an excution of rtasks.\
            \ttype of task factory: {}",
            factory_name,
        );
        let entry_tasks = recovery.init();
        (Progress::default(), entry_tasks)
    } else {
        info!("Recover an excution of rtasks.\
            \ttype of task factory: {}",
            factory_name,
        );
        let snapshot = recover(&bucket).await;
        let (progress, entry_tasks) = recovery.recover(snapshot);
        (progress, entry_tasks)
    };
    let (prog, batch) = deal_with_new_tasks(&entry_tasks);
    progress += &prog;
    bucket.transaction::<_, kv::Error, _>(move |txn| {
        txn.batch(&batch).unwrap();
        Ok(())
    }).unwrap();
    let (task_tx, mut task_rx) = mpsc::channel(128);
    spawn_all_tasks(task_tx.clone(), entry_tasks, &factory_name);
    loop {
        debug!("Task execution progresses a bit.\
            \tprogress: {:?}\
            \ttype of task factory: {}",
            progress,
            factory_name,
        );
        if let Err(_) = progress_sender.send(progress.clone()) {
            debug!("All subscribers of progress are gone.\
                \ttype of task factory: {}",
                factory_name,
            );
        }
        if progress.is_finished() {
            break;
        }
        let (task, res) = if let Some((task, res)) = task_rx.recv().await {
            (task, res)
        } else {
            error!("Tasks are unexpectedly completed.\
                \ttype of task factory: {}",
                factory_name,
            );
            break;
        };
        match res {
            Err(err) => {
                error!("A rtask failed.\
                    \ttask: {}\
                    \ttask factory: {}\
                    \terror: {:?}",
                    task.id(),
                    factory_name,
                    err,
                );
            }
            Ok(res) => {
                debug!("A rtask is completed.\
                    \ttask: {}\
                    \ttask factory: {}",
                    task.id(),
                    factory_name,
                );
                let (prog, mut batch) = deal_with_new_tasks(&res.successors);
                progress += &prog;
                progress.completed_tasks += 1;
                progress.completed_measure += task.measure();
                let val = wrap_value(
                    task.init_data().as_ref(), 
                    Some(res.result.as_ref()),
                    &res.successors,
                );
                batch.set(task.id().as_bytes().as_ref(), val.as_ref()).unwrap();
                bucket.transaction::<_, kv::Error, _>(move |txn| {
                    txn.batch(&batch).unwrap();
                    Ok(())
                }).unwrap();
                spawn_all_tasks(task_tx.clone(), res.successors, factory_name);
            }
        }
    }
    debug!("Task execution is going to shutdown.\
        \ttype of task factory: {}",
        factory_name,
    );
    bucket.flush_async().await.unwrap();
}

async fn recover<'a>(
    db: &kv::Bucket<'a, kv::Raw, kv::Raw>,
)  -> BTreeMap<RTaskId, RecoveredData> {
    let mut snapshot = BTreeMap::new();
    for item in db.iter() {
        let item = item.unwrap();
        let key: kv::Raw = item.key().unwrap();
        let value = item.value::<kv::Raw>().unwrap();
        let id = RTaskId::recover(Bytes::copy_from_slice(key.as_ref()));
        let value = match avro_rs::from_avro_datum(&VALUE_SCHEMA, &mut value.as_ref(), None) {
            Ok(v) => v,
            Err(err) => {
                error!("Error on deserializing recovery data. Please remove the entire database.\
                    \terror: {:?}",
                    err,
                );
                panic!("Error on deserializing recovery data. Please remove the entire database.");
            }
        };
        let data: RecoveredData = value.try_into().unwrap();
        snapshot.insert(id, data);
    }
    snapshot
}

fn deal_with_new_tasks(
    tasks: &Vec<Box<dyn RTask + Send>>,
) -> (Progress, kv::Batch::<kv::Raw, kv::Raw>) {
    for x in tasks.iter() {
        debug!("A new rtask is generated.\
            \ttask: {}",
            x.id(),
        );
    }
    let progress = tasks.iter()
        .map(|x| {
            let mut prog = Progress::default();
            prog.total_tasks += 1;
            prog.total_measure += x.measure();
            prog
        })
        .sum();
    let mut batch = kv::Batch::new();
    for x in tasks.iter() {
        let val = wrap_value(x.init_data().as_ref(), None, &[]);
        batch.set(x.id().as_bytes().as_ref(), val.as_ref()).unwrap();
    }
    (progress, batch)
}

fn spawn_all_tasks(
    task_tx: mpsc::Sender<(Box<dyn RTask + Send>, Result<RTaskResult, Error>)>,
    tasks: Vec<Box<dyn RTask + Send>>,
    factory_name: &str,
) {
    let cnt = tasks.len();
    for x in tasks.into_iter() {
        let task_tx = task_tx.clone();
        tokio::spawn(async move {
            go_rtask(x, task_tx).await;
        });
    }
    debug!("Some tasks are spawned.\
        \ttype of task factory: {}\
        \tcount: {}",
        factory_name,
        cnt,
    );
}

async fn go_rtask(
    mut task: Box<dyn RTask + Send>,
    resp: mpsc::Sender<(Box<dyn RTask + Send>, Result<RTaskResult, Error>)>,
) {
    debug!("A rtask is ready to go.\
        \ttask: {}",
        task.id(),
    );
    let res = task.complete().await;
    debug!("A rtask is executed.\
        \ttask: {}",
        task.id(),
    );
    resp.send((task, res)).await
        .unwrap_or_else(|_| {
            error!("Unexpected error on sending into a mpsc queue.");
        });
}

const VALUE_SCHEMA_STR: &str = r#"
{
    "type": "record",
    "name": "value",
    "fields": [
        {"name": "init", "type": "bytes"},
        {"name": "result", "type": ["null", "bytes"]},
        {"name": "successors", "type": {"type": "array", "items": "bytes"}}
    ]
}
"#;

#[dynamic(lazy)]
static VALUE_SCHEMA: avro_rs::Schema = avro_rs::Schema::parse_str(VALUE_SCHEMA_STR).unwrap();

fn wrap_value(
    init: &[u8],
    result: Option<&[u8]>,
    successors: &[Box<dyn RTask + Send>],
) -> Bytes {
    let mut rec = avro::Record::new(&VALUE_SCHEMA).unwrap();
    {
        let val = Vec::from(init);
        rec.put("init", avro::Value::Bytes(val));
    }
    if let Some(result) = result {
        let val = avro::Value::Bytes(Vec::from(result));
        let val = avro::Value::Union(Box::new(val));
        rec.put("result", val);
    } else {
        let val = avro::Value::Null;
        let val = avro::Value::Union(Box::new(val));
        rec.put("result", val);
    }
    {
        let val: Vec<_> = successors.iter()
            .map(|x| {
                let id = x.id();
                let val = Vec::from(id.as_bytes().as_ref());
                avro::Value::Bytes(val)
            })
            .collect();
        rec.put("successors", avro::Value::Array(val));
    }
    match avro_rs::to_avro_datum(&VALUE_SCHEMA, rec) {
        Ok(bs) => {
            Bytes::from(bs)
        }
        Err(err) => {
            error!("Error on serialize init data and result of rtask.\
                \terror: {:?}",
                err,
            );
            panic!("Error on serialize init data and result of rtask.\
                \terror: {:?}",
                err,
            )
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Progress {
    pub total_tasks: usize,
    pub completed_tasks: usize,
    pub error_tasks: usize,
    pub total_measure: usize,
    pub completed_measure: usize,
    pub error_measure: usize,
}

impl Progress {
    fn is_finished(&self) -> bool {
        self.completed_tasks + self.error_tasks >= self.total_tasks
    }
}

impl std::ops::AddAssign<&Progress> for Progress {
    fn add_assign(&mut self, rhs: &Progress) {
        self.total_tasks += rhs.total_tasks;
        self.completed_tasks += rhs.completed_tasks;
        self.error_tasks += rhs.error_tasks;
        self.total_measure += rhs.total_measure;
        self.completed_measure += rhs.completed_measure;
        self.error_measure += rhs.error_measure;
    }
}

impl std::iter::Sum for Progress {
    fn sum<I: Iterator<Item = Self>>(iter: I) -> Self {
        let mut res = Progress::default();
        iter.for_each(|x| {
            res += &x;
        });
        res
    }
}

#[cfg(test)]
mod tests {
    use crate::rtask::*;
    use log::*;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use stdext::function_name;
    use tokio::sync::Barrier;

    #[tokio::test]
    async fn single_task() {
        let path = PathBuf::from(format!("/dev/shm/{}.db", function_name!()));
        let log = Arc::new(std::sync::Mutex::new(vec![]));
        let log1 = log.clone();
        let recovery = test_tasks::NoRecovery{
            gen_entries: Box::new(move || {
                let res = Box::new(test_tasks::Task0::new(log1.clone()));
                vec![res as Box<dyn RTask + Send>]
            })
        };
        let ctrl = Controller::new(&path, recovery).unwrap();
        let progress = ctrl.wait_until_completion().await;
        let events = {
            let xs = log.lock().unwrap();
            xs.clone()
        };
        assert_eq!(events, vec![
            "init Task0".to_string(),
            "complete Task0".to_string(),
        ]);
        assert_eq!(
            progress, 
            Some(Progress{
                total_tasks: 1,
                completed_tasks: 1,
                error_tasks: 0,
                total_measure: 1,
                completed_measure: 1,
                error_measure: 0,
            }),
        );
    }

    #[tokio::test]
    async fn two_stage_task() {
        let path = PathBuf::from(format!("/dev/shm/{}.db", function_name!()));
        let log = Arc::new(std::sync::Mutex::new(vec![]));
        let log1 = log.clone();
        let recovery = test_tasks::NoRecovery{
            gen_entries: Box::new(move || {
                let res = Box::new(test_tasks::Task1::new(log1.clone()));
                vec![res as Box<dyn RTask + Send>]
            })
        };
        let ctrl = Controller::new(&path, recovery).unwrap();
        ctrl.wait_until_completion().await;
        let events = {
            let xs = log.lock().unwrap();
            xs.clone()
        };
        assert_eq!(events, vec![
            "init Task1".to_string(),
            "init Task0".to_string(),
            "complete Task1".to_string(),
            "complete Task0".to_string()]);
    }

    #[tokio::test]
    async fn parallel_execution() {
        let path = PathBuf::from(format!("/dev/shm/{}.db", function_name!()));
        const PARALLEL: usize = 4;
        let barrier = Arc::new(Barrier::new(PARALLEL + 1));
        let barrier1 = barrier.clone();
        let recovery = test_tasks::NoRecovery{
            gen_entries: Box::new(move || {
                (0..PARALLEL)
                    .map(|_| {
                        let res = test_tasks::BarrierTask{
                            id: RTaskId::new(),
                            completion: barrier1.clone(),
                        };
                        Box::new(res) as Box<dyn RTask + Send>
                    })
                    .collect::<Vec<_>>()
            })
        };
        let ctrl = Controller::new(&path, recovery).unwrap();
        barrier.wait().await;
        // This case is considered passed when it comes here.
        // The fact that the barrier is larger than 2, implies more than one 
        // tasks are parallelly running.
        ctrl.wait_until_completion().await;
    }

    #[tokio::test]
    async fn recovery() {
        let path = PathBuf::from(format!("/dev/shm/{}.db", function_name!()));
        {
            let log = Arc::new(std::sync::Mutex::new(vec![]));
            let mut task2 = test_tasks::Task2::new(log.clone());
            let res = task2.real_complete().await.unwrap();
            assert_eq!(res.successors.len(), 1);
            let task1 = &res.successors[0];
            save_recovery_data(&path, &task2, Some(res.result.as_ref()), &res.successors).await;
            save_recovery_data(&path, task1.as_ref(), None, &[]).await;
            info!("task2:\
                \tid: {:?}\
                \tinit: {:?}\
                \tresult: {:?}\
                \tsucc: {:?}",
                task2.id(),
                task2.init_data(),
                res.result,
                task1.id(),
            );
        }
        let log = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
        let factory = test_tasks::TrialRecovery{
            log: log.clone(),
        };
        let factory = DefaultImplFactory::new(factory);
        let ctrl = Controller::new(&path, factory).unwrap();
        let progress = ctrl.wait_until_completion().await;
        let events = {
            let xs = log.lock().unwrap();
            xs.clone()
        };
        assert_eq!(events, vec![
            "recover completed Task2".to_string(),
            "recover Task1".to_string(),
            "init Task0".to_string(),
            "complete Task1".to_string(),
            "complete Task0".to_string(),
        ]);
        assert_eq!(progress,
            Some(Progress{
                total_tasks: 3,
                completed_tasks: 3,
                error_tasks: 0,
                total_measure: 7,
                completed_measure: 7,
                error_measure: 0,
            })
        );
    }

    async fn save_recovery_data(
        path: &Path,
        task: &dyn RTask,
        result: Option<&[u8]>,
        successors: &[Box<dyn RTask + Send>],
    ) {
        let db_cfg = kv::Config::new(path);
        let db = kv::Store::new(db_cfg).unwrap();
        let bucket = db.bucket::<kv::Raw, kv::Raw>(Some(super::BUCKET_NAME)).unwrap();
        let mut batch = kv::Batch::<kv::Raw, kv::Raw>::new();
        let val = super::wrap_value(task.init_data().as_ref(), result, successors);
        batch.set(task.id().as_bytes().as_ref(), val.as_ref()).unwrap();
        bucket.batch(batch).unwrap();
        bucket.flush_async().await.unwrap();
    }

    mod test_tasks {
        use core::panic;
        use std::{collections::BTreeMap, sync::Arc};
        use tokio::sync::Barrier;
        use crate::rtask::*;

        const TASK0_MAGIC: &[u8; 5] = b"Task0";
        const TASK1_MAGIC: &[u8; 5] = b"Task1";
        const TASK2_MAGIC: &[u8; 5] = b"Task2";

        pub struct Task0 {
            id: RTaskId,
            log: Arc<std::sync::Mutex<Vec<String>>>,
        }

        impl Task0 {
            pub fn new(log: Arc<std::sync::Mutex<Vec<String>>>) -> Self {
                {
                    let mut xs = log.lock().unwrap();
                    xs.push("init Task0".to_string());
                }
                Self{
                    id: RTaskId::new(),
                    log,
                }
            }

            pub fn recover_incompleted(
                id: RTaskId,
                log: Arc<std::sync::Mutex<Vec<String>>>,
            ) -> Self {
                {
                    let mut xs = log.lock().unwrap();
                    xs.push("recover Task0".to_string());
                }
                Self{
                    id,
                    log,
                }
            }

            pub fn recover_completed(
                log: Arc<std::sync::Mutex<Vec<String>>>,
            ) -> Progress {
                {
                    let mut xs = log.lock().unwrap();
                    xs.push("recover completed Task0".to_string());
                }
                Progress{
                    total_tasks: 1,
                    completed_tasks: 1,
                    error_tasks: 0,
                    total_measure: 1,
                    completed_measure: 1,
                    error_measure: 0,
                }
            }

            async fn real_complete(&mut self) -> Result<RTaskResult, Error> {
                let res = RTaskResult{
                    result: Bytes::new(),
                    successors: vec![],
                };
                {
                    let mut xs = self.log.lock().unwrap();
                    xs.push("complete Task0".to_string());
                }
                Ok(res)
            }
        }

        impl RTask for Task0 {
            fn complete<'a>(&'a mut self) -> Pin<Box<dyn Future<Output=Result<RTaskResult, Error>> + Send + 'a>> {
                let r = self.real_complete();
                Box::pin(r)
            }

            fn init_data(&self) -> Bytes {
                Bytes::from_static(TASK0_MAGIC)
            }

            fn measure(&self) -> usize {
                1
            }

            fn id(&self) -> &RTaskId {
                &self.id
            }
        }

        pub struct Task1 {
            id: RTaskId,
            log: Arc<std::sync::Mutex<Vec<String>>>,
        }

        impl Task1 {
            pub fn new(log: Arc<std::sync::Mutex<Vec<String>>>) -> Self {
                {
                    let mut xs = log.lock().unwrap();
                    xs.push("init Task1".to_string());
                }
                Self{
                    id: RTaskId::new(),
                    log,
                }
            }

            pub fn recover_incompleted(
                id: RTaskId,
                log: Arc<std::sync::Mutex<Vec<String>>>,
            ) -> Self {
                {
                    let mut xs = log.lock().unwrap();
                    xs.push("recover Task1".to_string());
                }
                Self{
                    id,
                    log,
                }
            }

            pub fn recover_completed(
                log: Arc<std::sync::Mutex<Vec<String>>>,
            ) -> Progress {
                {
                    let mut xs = log.lock().unwrap();
                    xs.push("recover completed Task1".to_string());
                }
                Progress{
                    total_tasks: 1,
                    completed_tasks: 1,
                    error_tasks: 0,
                    total_measure: 2,
                    completed_measure: 2,
                    error_measure: 0,
                }
            }

            async fn real_complete(&mut self) -> Result<RTaskResult, Error> {
                let res = RTaskResult{
                    result: Bytes::new(),
                    successors: vec![
                        Box::new(Task0::new(self.log.clone())),
                    ],
                };
                {
                    let mut xs = self.log.lock().unwrap();
                    xs.push("complete Task1".to_string());
                }
                Ok(res)
            }
        }

        impl RTask for Task1 {
            fn complete<'a>(&'a mut self) -> Pin<Box<dyn Future<Output=Result<RTaskResult, Error>> + Send + 'a>> {
                let r = self.real_complete();
                Box::pin(r)
            }

            fn init_data(&self) -> Bytes {
                Bytes::from_static(TASK1_MAGIC)
            }

            fn measure(&self) -> usize {
                2
            }

            fn id(&self) -> &RTaskId {
                &self.id
            }
        }

        pub struct Task2 {
            id: RTaskId,
            log: Arc<std::sync::Mutex<Vec<String>>>,
        }

        impl Task2 {
            pub fn new(log: Arc<std::sync::Mutex<Vec<String>>>) -> Self {
                {
                    let mut xs = log.lock().unwrap();
                    xs.push("init Task2".to_string());
                }
                Self{
                    id: RTaskId::new(),
                    log,
                }
            }

            pub fn recover_incompleted(
                id: RTaskId,
                log: Arc<std::sync::Mutex<Vec<String>>>,
            ) -> Self {
                {
                    let mut xs = log.lock().unwrap();
                    xs.push("recover Task2".to_string());
                }
                Self{
                    id,
                    log,
                }
            }

            pub fn recover_completed(
                log: Arc<std::sync::Mutex<Vec<String>>>,
            ) -> Progress {
                {
                    let mut xs = log.lock().unwrap();
                    xs.push("recover completed Task2".to_string());
                }
                Progress{
                    total_tasks: 1,
                    completed_tasks: 1,
                    error_tasks: 0,
                    total_measure: 4,
                    completed_measure: 4,
                    error_measure: 0,
                }
            }

            pub async fn real_complete(&mut self) -> Result<RTaskResult, Error> {
                let res = RTaskResult{
                    result: Bytes::new(),
                    successors: vec![
                        Box::new(Task1::new(self.log.clone())),
                    ],
                };
                {
                    let mut xs = self.log.lock().unwrap();
                    xs.push("complete Task2".to_string());
                }
                Ok(res)
            }
        }

        impl RTask for Task2 {
            fn complete<'a>(&'a mut self) -> Pin<Box<dyn Future<Output=Result<RTaskResult, Error>> + Send + 'a>> {
                let r = self.real_complete();
                Box::pin(r)
            }

            fn init_data(&self) -> Bytes {
                Bytes::from_static(TASK2_MAGIC)
            }

            fn measure(&self) -> usize {
                4
            }

            fn id(&self) -> &RTaskId {
                &self.id
            }
        }

        impl BarrierTask {
            async fn real_complete(&mut self) -> Result<RTaskResult, Error> {
                let res = RTaskResult{
                    result: Bytes::new(),
                    successors: vec![],
                };
                self.completion.wait().await;
                Ok(res)
            }
        }

        impl RTask for BarrierTask {
            fn complete<'a>(&'a mut self) -> Pin<Box<dyn Future<Output=Result<RTaskResult, Error>> + Send + 'a>> {
                let r = self.real_complete();
                Box::pin(r)
            }

            fn init_data(&self) -> Bytes {
                Bytes::from_static(b"1")
            }

            fn measure(&self) -> usize {
                1
            }

            fn id(&self) -> &RTaskId {
                &self.id
            }
        }

        pub struct NoRecovery {
            pub gen_entries: Box<dyn Fn() -> Vec<Box<dyn RTask + Send>> + Send + 'static>,
        }
        
        impl Factory for NoRecovery {
            fn init(&self) -> Vec<Box<dyn RTask + Send>> {
                (self.gen_entries)()
            }

            fn recover(
                &self, 
                _snapshot: BTreeMap<RTaskId, RecoveredData>,
            ) -> (Progress, Vec<Box<dyn RTask + Send>>) {
                panic!()
            }
        }

        pub struct BarrierTask {
            pub id: RTaskId,
            pub completion: Arc<Barrier>,
        }

        pub struct TrialRecovery {
            pub log: Arc<std::sync::Mutex<Vec<String>>>,
        }

        impl FactoryForDefaultImpl for TrialRecovery {
            fn init(&self) -> Vec<Box<dyn RTask + Send>> {
                panic!()
            }

            fn recover_completed(
                &self,
                _id: RTaskId,
                init: &[u8],
                _result: &[u8],
            ) -> Progress {
                match init {
                    x if &x == &TASK0_MAGIC => {
                        Task0::recover_completed(self.log.clone())
                    }
                    x if &x == &TASK1_MAGIC => {
                        Task1::recover_completed(self.log.clone())
                    }
                    x if &x == &TASK2_MAGIC => {
                        Task2::recover_completed(self.log.clone())
                    }
                    x @ _ => {
                        panic!("{:?}", x)
                    }
                }
            }
        
            fn recover_incompleted(
                &self,
                id: RTaskId,
                init: &[u8],
            ) -> Box<dyn RTask + Send> {
                match init.as_ref() {
                    x if &x == &TASK0_MAGIC => {
                        let v = Task0::recover_incompleted(id, self.log.clone());
                        Box::new(v) as Box<dyn RTask + Send>
                    }
                    x if &x == &TASK1_MAGIC => {
                        let v = Task1::recover_incompleted(id, self.log.clone());
                        Box::new(v) as Box<dyn RTask + Send>
                    }
                    x if &x == &TASK2_MAGIC => {
                        let v = Task2::recover_incompleted(id, self.log.clone());
                        Box::new(v) as Box<dyn RTask + Send>
                    }
                    x @ _ => {
                        panic!("{:?}", x)
                    }
                }
            }
        }

    }
}
