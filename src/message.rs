use serde::de::{self, DeserializeOwned, Deserializer};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(bound(deserialize = "Arc<UserMessage<T>>: Deserialize<'de>"))]
pub enum PollerMessage<T> {
    UserMessage(Arc<UserMessage<T>>),
    PollEnded,
}

#[derive(Deserialize, Debug, Clone, Serialize)]
pub struct UserMessage<T> {
    pub global_id: i64,
    pub message_id: i64,
    pub channel: String,
    pub data: T,
}

#[derive(Debug, Clone)]
pub enum Data<T>
where
    T: Clone,
{
    Status(HashMap<String, i64>),
    Normal(UserMessage<T>),
}

impl<'de, T> Deserialize<'de> for Data<T>
where
    T: DeserializeOwned + Clone,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct RawMessage {
            global_id: i64,
            message_id: i64,
            channel: String,
            data: Value,
        }

        let raw = RawMessage::deserialize(deserializer)?;

        if raw.channel == "/__status" {
            let map: HashMap<String, i64> = serde_json::from_value(raw.data)
                .map_err(|e| de::Error::custom(format!("invalid status data format: {}", e)))?;
            Ok(Data::Status(map))
        } else {
            let data: T = serde_json::from_value::<T>(raw.data)
                .map_err(|e| de::Error::custom(format!("invalid normal data format: {}", e)))?;
            Ok(Data::Normal(UserMessage {
                global_id: raw.global_id,
                message_id: raw.message_id,
                channel: raw.channel,
                data,
            }))
        }
    }
}
