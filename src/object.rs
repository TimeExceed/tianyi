use std::fmt::Display;
use sha1::{Sha1, Digest};
use bytes::Bytes;

#[derive(Clone, Ord, PartialOrd, PartialEq, Eq)]
pub struct ObjectId(Bytes);

impl ObjectId {
    pub fn new(raw: Bytes) -> ObjectId {
        ObjectId(raw)
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

impl Display for ObjectId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ObjectId({})", self.to_readable())
    }
}

impl std::fmt::Debug for ObjectId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ObjectId({})", self.to_readable())
    }
}

#[derive(Debug, Clone)]
pub struct Object {
    id: ObjectId,
    content: Bytes,
}

impl Object {
    pub fn new(content: Bytes) -> Object {
        let id = {
            let mut hasher = Sha1::new();
            hasher.update(content.as_ref());
            let digest = hasher.finalize();
            ObjectId::new(Bytes::copy_from_slice(&digest))
        };
        Object{
            id,
            content,
        }
    }

    pub fn content(&self) -> &Bytes {
        &self.content
    }

    pub fn id(&self) -> &ObjectId {
        &self.id
    }
}

impl Display for Object {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Object({})", self.id.to_readable())
    }
}

impl std::cmp::Ord for Object {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.id.cmp(&other.id)
    }
}

impl std::cmp::PartialOrd for Object {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Object {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for Object {}

#[cfg(test)]
impl quickcheck::Arbitrary for Object {
    fn arbitrary(g: &mut quickcheck::Gen) -> Self {
        let content = Vec::<u8>::arbitrary(g);
        Object::new(Bytes::from(content))
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        let content = self.content().clone();
        Box::new(
            std::iter::successors(Some(content.len()), |pred| {
                if *pred == 0 {
                    None
                } else {
                    Some(pred / 2)
                }
            })
                .map(move |start| {
                    let xs = content[..start].to_owned();
                    Object::new(Bytes::from(xs))
                })
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn object_id() {
        let trial = Bytes::from(vec![1u8, 2u8, 0x1Fu8]);
        let trial = ObjectId::new(trial);
        assert_eq!(format!("{}", trial), "ObjectId(01021F)");
    }

    #[test]
    fn object() {
        let content = b"hello world";
        let content = Bytes::from_static(content);
        let obj = Object::new(content.clone());
        assert_eq!(obj.content, content);
        assert_eq!(format!("{}", obj), "Object(2AAE6C35C94FCFB415DBE95F408B9CE91EE846ED)");
    }
}
