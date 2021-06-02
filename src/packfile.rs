use crate::*;
use bytes::{Bytes, BytesMut};
use tokio::fs::{OpenOptions, File};
use tokio::io::{AsyncReadExt, AsyncWriteExt, AsyncSeekExt};
use std::cmp::min;
use std::collections::BTreeMap;
use std::io::SeekFrom;
use std::future::Future;
use std::path::{Path, PathBuf};
use log::{error, warn, info, debug};

pub struct PackFileWriter {
}

impl PackFileWriter {
    async fn dump<'a, ObjIter>(
        filename: &Path,
        objs: ObjIter,
    ) -> Result<(), std::io::Error> 
    where ObjIter: Iterator<Item=&'a Object> + Clone {
        info!("Write a pack file.\
            \tfilename: {:#?}\
            \t#objects: {}",
            filename,
            objs.clone().count(),
        );
        objs.clone()
            .zip(objs.clone().skip(1))
            .for_each(|(x, y)| {
                assert!(x < y);
            });
        let mut writer = PackFileWriterImpl::new(filename).await?;
        writer.write_all(HEADER).await?;
        let len = objs.clone().count();
        writer.write_u64(len as u64).await?;
        for x in objs.clone() {
            writer.write_all(x.id().as_bytes()).await?;
            writer.write_u64(x.content().len() as u64).await?;
        }
        for x in objs {
            writer.write_all(x.content().as_ref()).await?;
        }
        writer.shutdown().await?;
        Ok(())
    }
}

struct PackFileWriterImpl<'a> {
    file: tokio::fs::File,
    digester: crc::Digest<'a, u64>,
}

impl<'a> PackFileWriterImpl<'a> {
    async fn new(
        filename: &Path,
    ) -> Result<PackFileWriterImpl<'a>, std::io::Error> {
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(filename)
            .await?;
        Ok(PackFileWriterImpl{
            file,
            digester: CRC_64_XZ.digest(),
        })
    }

    async fn write_all(&mut self, buf: &[u8]) -> Result<(), std::io::Error> {
        self.file.write_all(buf).await?;
        self.digester.update(buf);
        Ok(())
    }

    async fn write_u32(&mut self, v: u32) -> Result<(), std::io::Error> {
        let buf = v.to_be_bytes();
        self.write_all(&buf).await?;
        Ok(())
    }

    async fn write_u64(&mut self, v: u64) -> Result<(), std::io::Error> {
        let buf = v.to_be_bytes();
        self.write_all(&buf).await?;
        Ok(())
    }

    async fn shutdown(mut self) -> Result<(), std::io::Error> {
        let checksum = self.digester.finalize();
        self.file.write_u64(checksum).await?;
        self.file.shutdown().await?;
        Ok(())
    }
}

const HEADER: &[u8; 8] = b"TIANYIv1";
const CRC_64_XZ: crc::Crc::<u64> = crc::Crc::<u64>::new(&crc::CRC_64_XZ);
const CRC_LEN: usize = 8;
const ID_LEN: usize = 20;

pub async fn check_integrity(path: &Path) -> Result<(), std::io::Error> {
    let mut fp = File::open(path).await?;
    let meta = fp.metadata().await?;
    if (meta.len() as usize) < CRC_LEN {
        error!("Pack file is required to contain a CRC-64/XZ checksum.\
            \tfilename: {:#?}\
            \tlength: {}",
            path,
            meta.len(),
        );
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof, 
            "Pack file is required to contain a CRC-64/XZ checksum."
        ));
    }
    let expected_checksum = read_checksum(&mut fp).await?;
    let real_checksum = compute_checksum(&mut fp, meta.len() as usize).await?;
    if expected_checksum != real_checksum {
        error!("A pack file is corrupted.\
            \tfilename: {:#?}",
            path,
        );
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData, 
            "Pack file is corrupted."
        ));
    }
    Ok(())
}

async fn read_checksum(
    file: &mut File, 
) -> Result<u64, std::io::Error> {
    file.seek(SeekFrom::End(-(CRC_LEN as i64))).await?;
    let res = file.read_u64().await?;
    Ok(res)
}

async fn compute_checksum(
    file: &mut File,
    mut len: usize,
) -> Result<u64, std::io::Error> {
    file.seek(SeekFrom::Start(0)).await?;
    let mut digester = CRC_64_XZ.digest();
    let mut buf = BytesMut::with_capacity(4 * 1024);
    assert!(len >= CRC_LEN);
    len -= CRC_LEN;
    while len > 0 {
        let consumed = file.read_buf(&mut buf).await?;
        // the tailing block contains the checksum, which should not be summed.
        let consumed = min(consumed, len); 
        digester.update(&buf.as_ref()[..consumed]);
        len -= consumed;
        buf.clear();
    }
    Ok(digester.finalize())
}

#[derive(Debug)]
pub struct PackFileReader {
    path: PathBuf,
    metadata: BTreeMap<ObjectId, Metadata>,
}

impl PackFileReader {
    pub async fn new(path: PathBuf) -> Result<PackFileReader, std::io::Error> {
        check_integrity(&path).await?;
        let metadata = read_metadata(&path).await?;
        let metadata: BTreeMap<_, _> = metadata.into_iter()
            .map(|x| {
                (x.id.clone(), x)
            })
            .collect();
        Ok(PackFileReader{
            path,
            metadata,
        })
    }

    pub fn iter_meta(&self) -> impl Iterator<Item=&Metadata> {
        self.metadata.values()
    }

    pub async fn fetch(&self, id: &ObjectId) -> Result<Option<Bytes>, std::io::Error> {
        if let Some(meta) = self.metadata.get(id) {
            let mut fp = File::open(&self.path).await?;
            fp.seek(SeekFrom::Start(meta.offset as u64)).await?;
            let mut buf = Vec::<u8>::with_capacity(meta.len);
            buf.resize(meta.len, 0);
            fp.read_exact(&mut buf).await?;
            Ok(Some(Bytes::from(buf)))
        } else {
            Ok(None)
        }
    }
}

async fn read_metadata(path: &Path) -> Result<Vec<Metadata>, std::io::Error> {
    let mut fp = File::open(path).await?;
    fp.seek(SeekFrom::Start(HEADER.len() as u64)).await?;
    let len = fp.read_u64().await? as usize;
    let mut res = vec![];
    for _ in 0..len {
        let mut id = Vec::<u8>::with_capacity(ID_LEN);
        id.resize(ID_LEN, 0);
        fp.read_exact(&mut id).await?;
        let len = fp.read_u64().await?;
        let meta = Metadata{
            id: ObjectId::new(Bytes::from(id)),
            offset: 0,
            len: len as usize,
        };
        res.push(meta);
    }
    let mut offset: usize = HEADER.len() 
        + std::mem::size_of::<u64>() // number
        + len * (ID_LEN + std::mem::size_of::<u64>()); // indices
    res.iter_mut()
        .for_each(|x| {
            x.offset = offset;
            offset += x.len;
        });
    Ok(res)
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Metadata {
    id: ObjectId,
    offset: usize,
    len: usize,
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use super::*;
    use hex_literal::hex;
    use quickcheck::{TestResult, quickcheck};

    pub async fn read_file(path: &Path) -> Result<Bytes, std::io::Error> {
        let mut contents = vec![];
        let mut fp = File::open(path).await?;
        fp.read_to_end(&mut contents).await?;
        Ok(Bytes::from(contents))
    }
    
    pub struct AutoRemoveFile {
        path: PathBuf,
        suppress: bool,
    }
    
    impl AutoRemoveFile {
        pub fn new(path: &str) -> AutoRemoveFile {
            let path: PathBuf = ["/", "dev", "shm", path].iter().collect();
            AutoRemoveFile{
                path,
                suppress: false,
            }
        }
    
        #[allow(dead_code)]
        pub fn suppress(path: &str) -> AutoRemoveFile {
            let path: PathBuf = ["/", "dev", "shm", path].iter().collect();
            AutoRemoveFile{
                path,
                suppress: true,
            }
        }
    }
    
    impl AsRef<Path> for AutoRemoveFile {
        fn as_ref(&self) -> &Path {
            &self.path
        }
    }
    
    impl Drop for AutoRemoveFile {
        fn drop(&mut self) {
            if !self.suppress {
                std::fs::remove_file(self.as_ref()).unwrap();
            }
        }
    }

    #[tokio::test]
    async fn empty() {
        let path = AutoRemoveFile::new("empty.tianyi.pack");
        {
            let objs: Vec<Object> = vec![];
            PackFileWriter::dump(path.as_ref(), objs.iter()).await.unwrap();
        }
        let trial = read_file(path.as_ref()).await.unwrap();
        let oracle = {
            let mut oracle = vec![];
            oracle.extend_from_slice(HEADER); // header
            oracle.extend_from_slice(b"\0\0\0\0\0\0\0\0"); // number
            oracle.extend_from_slice(&hex!("41A31CEE35F0574A")); // checksum
            Bytes::from(oracle)
        };
        assert_eq!(trial, oracle);
    }

    #[tokio::test]
    async fn one() {
        let path = AutoRemoveFile::new("one.tianyi.pack");
        {
            let objs = vec![
                Object::new(Bytes::from_static(b"hello world")),
            ];
            PackFileWriter::dump(path.as_ref(), objs.iter()).await.unwrap();
        }
        let trial = read_file(path.as_ref()).await.unwrap();
        let oracle = {
            let mut oracle = vec![];
            oracle.extend_from_slice(HEADER); // header
            oracle.extend_from_slice(b"\0\0\0\0\0\0\0\x01"); // number
            oracle.extend_from_slice(&hex!("2AAE6C35C94FCFB415DBE95F408B9CE91EE846ED")); // SHA-1 of object #1
            oracle.extend_from_slice(&hex!("000000000000000B")); // length of object #1
            oracle.extend_from_slice(b"hello world"); // content of object #1
            oracle.extend_from_slice(&hex!("9941CC60CF2E3A30")); // checksum
            Bytes::from(oracle)
        };
        assert_eq!(trial, oracle);
    }

    #[tokio::test]
    async fn check_integrity_ok() {
        let path = AutoRemoveFile::new("check_integrity_ok.tianyi.pack");
        {
            let objs: Vec<Object> = vec![];
            PackFileWriter::dump(path.as_ref(), objs.iter()).await.unwrap();
        }
        check_integrity(path.as_ref()).await.unwrap();
    }

    #[tokio::test]
    async fn check_integrity_fail_too_short_file() {
        let path = AutoRemoveFile::new("check_integrity_fail_too_short_file.tianyi.pack");
        {
            let mut file = OpenOptions::new()
                .write(true)
                .create(true)
                .open(path.as_ref())
                .await
                .unwrap();
            file.shutdown().await.unwrap();
        }
        check_integrity(path.as_ref()).await.unwrap_err();
    }

    #[tokio::test]
    async fn check_integrity_fail_checksum_mismatch() {
        let path = AutoRemoveFile::new("check_integrity_fail_checksum_mismatch.tianyi.pack");
        {
            let mut file = OpenOptions::new()
                .write(true)
                .create(true)
                .open(path.as_ref())
                .await
                .unwrap();
            file.write_all(HEADER).await.unwrap(); // header
            file.write_all(b"\0\0\0\0\0\0\0\0").await.unwrap(); // number
            file.write_all(b"A`\x08W\x18\x05\xae^").await.unwrap(); // incorrect checksum
            file.shutdown().await.unwrap();
        }
        check_integrity(path.as_ref()).await.unwrap_err();
    }

    #[tokio::test]
    async fn read_empty_file() {
        let path = AutoRemoveFile::new("read_empty_file.tianyi.pack");
        {
            let objs: Vec<Object> = vec![];
            PackFileWriter::dump(path.as_ref(), objs.iter()).await.unwrap();
        }
        let rdr = PackFileReader::new(path.as_ref().to_owned()).await.unwrap();
        let meta: Vec<_> = rdr.iter_meta().collect();
        assert_eq!(meta, Vec::<&Metadata>::new());
    }

    #[tokio::test]
    async fn read_1obj_file() {
        let path = AutoRemoveFile::new("read_1obj_file.tianyi.pack");
        let oracle_obj = Object::new(Bytes::from_static(b"hello world"));
        {
            let objs = vec![oracle_obj.clone()];
            PackFileWriter::dump(path.as_ref(), objs.iter()).await.unwrap();
        }
        let rdr = PackFileReader::new(path.as_ref().to_owned()).await.unwrap();
        let meta: Vec<_> = rdr.iter_meta().collect();
        assert_eq!(meta, vec![
            &Metadata{
                id: (&oracle_obj).id().clone(),
                offset: 44,
                len: 11,
            }
        ]);
        assert_eq!(
            rdr.fetch(oracle_obj.id()).await.unwrap(), 
            Some(oracle_obj.content().clone()));
    }

    #[test]
    fn write_then_read() {
        quickcheck(write_then_read_stub as fn(BTreeSet<Object>) -> TestResult);
    }

    fn write_then_read_stub(objs: BTreeSet<Object>) -> TestResult {
        let rt  = tokio::runtime::Runtime::new().unwrap();
        let res = rt.block_on(async {
            match go_write_then_read(objs).await {
                Ok(x) => x,
                Err(err) => {
                    TestResult::error(err.to_string())
                }
            }
        });
        res
    }

    async fn go_write_then_read(
        objs: BTreeSet<Object>,
    ) -> Result<TestResult, std::io::Error> {
        let path = AutoRemoveFile::new("write_then_read.tianyi.pack");
        PackFileWriter::dump(path.as_ref(), objs.iter()).await?;
        let rdr = PackFileReader::new(path.as_ref().to_owned()).await?;
        let meta: Vec<_> = rdr.iter_meta().collect();
        {
            let real_ids: Vec<_> = meta.iter()
                .map(|x| {
                    x.id.clone()
                })
                .collect();
            let expected_ids: Vec<_> = objs.iter()
                .map(|x| {
                    x.id().clone()
                })
                .collect();
            if real_ids != expected_ids {
                return Ok(TestResult::failed());
            }
        }
        {
            let mut real_contents: Vec<Bytes> = vec![];
            for x in meta.iter() {
                let content = rdr.fetch(&x.id).await.unwrap();
                if let Some(content) = content {
                    real_contents.push(content);
                } else {
                    panic!("no content of {:?}", x.id);
                }
            }
            let expected_contents: Vec<_> = objs.iter()
                .map(|x| {
                    x.content().clone()
                })
                .collect();
            if real_contents != expected_contents {
                return Ok(TestResult::failed());
            }
        }
        Ok(TestResult::passed())
    }
}
