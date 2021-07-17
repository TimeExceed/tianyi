use std::collections::BTreeMap;
use super::*;

pub trait Factory {
    fn init(&self) -> Vec<Box<dyn RTask + Send>>;
    fn recover(
        &self, 
        snapshot: BTreeMap<RTaskId, RecoveredData>,
    ) -> (Progress, Vec<Box<dyn RTask + Send>>);
}

#[derive(Debug, Clone, Default)]
pub struct RecoveredData {
    pub init: Bytes,
    pub result: Option<Bytes>,
}

impl std::convert::TryFrom<avro_rs::types::Value> for RecoveredData {
    type Error = Error;

    fn try_from(x: avro_rs::types::Value) -> Result<Self, Self::Error> {
        let xs = match x {
            avro_rs::types::Value::Record(xs) => xs,
            x @ _ => {
                return Err(Error::RecoverData(x));
            }
        };
        let mut map: BTreeMap<_, _> = xs.into_iter().collect();
        let mut res = Self::default();
        if let Some(v) = map.remove("init") {
            match v {
                avro_rs::types::Value::Bytes(v) => {
                    res.init = Bytes::from(v);
                }
                v @ _ => {
                    return Err(Error::RecoverData(v));
                }
            }
        } else {
            return Err(Error::from_message(r#"field "init" is required."#));
        }

        if let Some(v) = map.remove("result") {
            let v = match v {
                avro_rs::types::Value::Union(v) => v,
                v @ _ => {
                    return Err(Error::RecoverData(v));
                }
            };
            match v.as_ref() {
                &avro_rs::types::Value::Bytes(ref v) => {
                    res.result = Some(Bytes::from(v.clone()));
                },
                &avro_rs::types::Value::Null => {},
                v @ _ => {
                    return Err(Error::RecoverData(v.clone()));
                }
            };
        } else {
            return Err(Error::from_message(r#"field "result" is required."#));
        }

        Ok(res)
    }
}

pub trait FactoryForDefaultImpl {
    fn init(&self) -> Vec<Box<dyn RTask + Send>>;
    
    fn recover_completed(
        &self,
        id: RTaskId,
        init: &[u8],
        result: &[u8],
    ) -> Progress;
    
    fn recover_incompleted(
        &self,
        id: RTaskId,
        init: &[u8],
    ) -> Box<dyn RTask + Send>;
}

pub struct DefaultImplFactory<Fac: FactoryForDefaultImpl + Send> {
    fac: Fac,
}

impl<Fac: FactoryForDefaultImpl + Send> DefaultImplFactory<Fac> {
    pub fn new(fac: Fac) -> Self {
        Self{
            fac,
        }
    }
}

impl<Fac: FactoryForDefaultImpl + Send> Factory for DefaultImplFactory<Fac> {
    fn init(&self) -> Vec<Box<dyn RTask + Send>> {
        self.fac.init()
    }

    fn recover(
        &self, 
        snapshot: BTreeMap<RTaskId, RecoveredData>,
    ) -> (Progress, Vec<Box<dyn RTask + Send>>) {
        let mut incompleted = BTreeMap::<RTaskId, Box<dyn RTask + Send>>::new();
        let mut progress = Progress::default();
        for (id, rdata) in snapshot.iter() {
            if let Some(result) = &rdata.result {
                let id = id.clone();
                let prog = self.fac.recover_completed(
                    id,
                    rdata.init.as_ref(),
                    result);
                progress += &prog;
            } else {
                let id = id.clone();
                let task = self.fac.recover_incompleted(
                    id.clone(), 
                    rdata.init.as_ref());
                incompleted.insert(id, task);
            }
        }
        let incompleted: Vec<_> = incompleted.into_iter()
            .map(|(_, v)| v)
            .collect();
        (progress, incompleted)
    }
}