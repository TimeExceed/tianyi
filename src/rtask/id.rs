use bytes::Bytes;
use std::cmp::max;
use std::convert::TryInto;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Clone, Ord, PartialOrd, PartialEq, Eq)]
pub struct RTaskId(Bytes);

static ID: AtomicU64 = AtomicU64::new(0);

impl RTaskId {
    pub fn new() -> RTaskId {
        let id = ID.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        RTaskId(Bytes::copy_from_slice(&id.to_be_bytes()))
    }

    pub fn recover(raw: Bytes) -> RTaskId {
        let current = read_from_bytes(raw.as_ref()) + 1;
        let mut old = ID.load(Ordering::Relaxed);
        loop {
            let new = max(current, old);
            let r = ID.compare_exchange_weak(
                old, 
                new, 
                Ordering::AcqRel, 
                Ordering::Relaxed);
            match r {
                Ok(_) => {
                    break;
                }
                Err(x) => {
                    old = x;
                }
            }
        }
        RTaskId(raw)
    }

    pub fn as_bytes(&self) -> &Bytes {
        &self.0
    }

    pub fn to_readable(&self) -> String {
        let mut res = String::new();
        self.0.iter()
            .map(|x| {
                format!("{:02X}", x)
            })
            .for_each(|x| {
                res.push_str(&x);
            });
        res
    }
}

fn read_from_bytes(input: &[u8]) -> u64 {
    let bs: [u8; std::mem::size_of::<u64>()] = match input.len() {
        x if x != std::mem::size_of::<u64>() => {
            panic!("Expect an u64, but got much more bytes: {}", x);
        }
        _ => {
            input.try_into().unwrap()
        }
    };
    u64::from_be_bytes(bs)
}

impl std::fmt::Display for RTaskId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TaskId({})", self.to_readable())
    }
}

impl std::fmt::Debug for RTaskId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TaskId({})", self.to_readable())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn task_id_are_all_different() {
        for _ in 0..1000 {
            let left = RTaskId::new();
            let right = RTaskId::new();
            assert_ne!(left, right);
        }
    }

    #[test]
    fn recover() {
        let old = 300u64;
        let bs = Bytes::copy_from_slice(&old.to_be_bytes());
        let old = RTaskId::recover(bs);
        let new = RTaskId::new();
        assert!(new > old, "new: {}, old: {}", new, old);
    }
}
