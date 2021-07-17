use bytes::Bytes;
use std::future::Future;
use std::pin::Pin;

mod id;
pub use self::id::*;

mod error;
pub use self::error::*;

mod controller;
pub use self::controller::*;

mod factory;
pub use self::factory::*;

pub trait RTask {
    fn complete<'a>(&'a mut self) -> Pin<Box<dyn Future<Output=Result<RTaskResult, Error>> + Send + 'a>>;
    fn init_data(&self) -> Bytes;
    fn measure(&self) -> usize;
    fn id(&self) -> &RTaskId;
}

pub struct RTaskResult {
    pub result: Bytes,
    pub successors: Vec<Box<dyn RTask + Send>>,
}

