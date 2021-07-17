#[derive(Debug)]
pub enum Error {
    Message(String),
    KV(kv::Error),
    Avro(avro_rs::Error),
    RecoverData(avro_rs::types::Value),
}

impl Error {
    pub fn from_message<S: ToString>(msg: S) -> Self {
        Self::Message(msg.to_string())
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Message(_) => None,
            Self::KV(ref err) => Some(err),
            Self::Avro(ref err) => Some(err),
            Self::RecoverData(_) => None,
        }
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Caused by {:?}", self)
    }
}

impl From<kv::Error> for Error {
    fn from(src: kv::Error) -> Self {
        Self::KV(src)
    }
}

impl From<avro_rs::Error> for Error {
    fn from(src: avro_rs::Error) -> Self {
        Self::Avro(src)
    }
}
