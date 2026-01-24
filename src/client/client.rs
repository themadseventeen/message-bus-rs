use std::collections::HashMap;
use std::marker::PhantomData;

use futures::{Stream, StreamExt};
use futures_util::TryStreamExt;
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::client::decoder::body_to_framed_stream;
use crate::{Message, client::client_id::ClientId};

pub struct Client<T> {
    url: String,
    client_id: ClientId,

    reqwest_client: reqwest::Client,

    _marker: PhantomData<T>,
}

impl Client<()> {
    pub fn builder() -> ClientBuilder {
        ClientBuilder::default()
    }
}

impl<T> Client<T>
where
    T: serde::de::DeserializeOwned + Send + 'static,
{
    pub async fn poll_once(
        &self,
    ) -> Result<impl Stream<Item = Result<Message<Value>, std::io::Error>>, reqwest::Error> {
        let target_url = format!("{}{}/poll", self.url, self.client_id.to_string());
        let mut form_data = HashMap::new();
        form_data.insert("/latest", "-1");

        let response = self
            .reqwest_client
            .post(&target_url)
            .form(&form_data)
            .send()
            .await?;

        use futures::stream;

        let s = body_to_framed_stream(response)
            .map(|val| {
                val.and_then(|v| {
                    serde_json::from_value::<Vec<Message<Value>>>(v)
                        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
                })
            })
            .map_ok(|vec| stream::iter(vec.into_iter().map(Ok)))
            .try_flatten();

        Ok(s)
    }
}

pub struct ClientBuilder {
    url: Option<String>,
    client_id: Option<ClientId>,

    reqwest_client: Option<reqwest::Client>,
}

impl Default for ClientBuilder {
    fn default() -> Self {
        ClientBuilder {
            url: None,
            client_id: None,
            reqwest_client: None,
        }
    }
}

impl ClientBuilder {
    pub fn url<S>(mut self, url: S) -> Self
    where
        S: Into<String>,
    {
        self.url = Some(url.into());
        self
    }

    pub fn with_id(mut self, id: ClientId) -> Self {
        self.client_id = Some(id);
        self
    }

    pub fn build<T>(self) -> Client<T>
    where
        T: DeserializeOwned + Send + 'static,
    {
        Client {
            url: match self.url {
                Some(url) => url,
                None => String::default(),
            },
            client_id: match self.client_id {
                Some(id) => id,
                None => ClientId::default(),
            },
            reqwest_client: match self.reqwest_client {
                Some(reqwest) => reqwest,
                None => reqwest::Client::default(),
            },
            _marker: PhantomData,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn client_test() -> anyhow::Result<()> {
        let client = Client::builder()
            .url("https://forum.example.com/message-bus/")
            .build::<Value>();

        let mut stream = client.poll_once().await?;

        use futures::StreamExt;

        while let Some(msg) = stream.next().await {
            match msg {
                Ok(message) => println!("{:?}", message),
                Err(e) => eprintln!("Stream error: {e}"),
            }
        }

        Ok(())
    }
}
