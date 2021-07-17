use std::path::{Path, PathBuf};

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
            match std::fs::remove_file(self.as_ref()) {
                Ok(_) => (),
                Err(err) => {
                    match err.kind() {
                        std::io::ErrorKind::NotFound => (),
                        _ => {
                            panic!("{:?}", err)
                        }
                    }
                }
            }
        }
    }
}

