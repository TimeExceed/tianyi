use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, SeekFrom};
use tianyi::define_str_var;

define_str_var!(ANYTHING, "ANYTHING");

#[tokio::main]
pub async fn main() {
    let mut file = File::open("README.md").await.unwrap();
    file.seek(SeekFrom::Start(2)).await.unwrap();
    let mut contents = vec![];
    file.read_to_end(&mut contents).await.unwrap();
    let contents = String::from_utf8(contents).unwrap();
    println!("{}", contents);
}
