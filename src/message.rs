use serde::Deserialize;

#[derive(Deserialize, Debug)]
pub struct Message<T> {
    pub global_id: i64,
    pub message_id: i64,
    pub channel: String,
    pub data: T,
}
