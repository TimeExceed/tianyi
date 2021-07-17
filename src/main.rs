use bytes::Bytes;
use static_init::{dynamic};

const VALUE_SCHEMA: &str = r#"
{
    "type": "record",
    "name": "value",
    "fields": [
        {"name": "init", "type": "bytes"},
        {
            "name": "result", 
            "type": ["null", "bytes"]
        }
    ]
}
"#;

#[dynamic(lazy)]
static SCHEMA: avro_rs::Schema = avro_rs::Schema::parse_str(VALUE_SCHEMA).unwrap();

#[tokio::main]
pub async fn main() {
    flexi_logger::Logger::try_with_str("info, mycrate=debug, tianyi=debug").unwrap()
        .format(flexi_logger::colored_with_thread)
        .start()
        .unwrap();
    println!("schema: {}", SCHEMA.canonical_form());
    let mut rec = avro_rs::types::Record::new(&SCHEMA).unwrap();
    rec.put("init", avro_rs::types::Value::Bytes(Vec::from(b"abc" as &[u8])));
    // rec.put("result", avro_rs::types::Value::Union(Box::new(avro_rs::types::Value::Null)));
    rec.put("result", avro_rs::types::Value::Union(Box::new(avro_rs::types::Value::Bytes(Vec::from(b"xyz" as &[u8])))));
    let bs = match avro_rs::to_avro_datum(&SCHEMA, rec) {
        Ok(bs) => {
            let res = Bytes::from(bs);
            println!("result: {:?}", res);
            res
        }
        Err(err) => {
            panic!("error: {:?}", err);
        }
    };
    let rec = avro_rs::from_avro_datum(&SCHEMA, &mut bs.as_ref(), None).unwrap();
    println!("{:?}", rec);
}
