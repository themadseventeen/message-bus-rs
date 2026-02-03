use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use futures::stream::BoxStream;
use serde::de::DeserializeOwned;
use serde_json::Value;
use tokio::sync::{
    Notify, RwLock,
    broadcast::{self, Sender},
};

use crate::{
    Message,
    client::{client_id::ClientId, decoder::body_to_framed_stream},
    message::MessageType,
};
use async_stream::stream;

pub struct Client<T>
where
    T: Clone + DeserializeOwned + Sync + Send,
{
    pub(crate) url: String,
    pub(crate) client_id: ClientId,
    pub(crate) reqwest_client: reqwest::Client,
    pub(crate) state: Arc<RwLock<ClientState>>,
    pub(crate) broadcast_tx: Sender<Result<Message<T>, Arc<std::io::Error>>>,
    pub(crate) poller_running: Arc<AtomicBool>,
    pub(crate) state_changed: Arc<Notify>,
    pub(crate) _phantom: std::marker::PhantomData<T>,
}

#[derive(Default)]
pub(crate) struct ClientState {
    pub(crate) subscriptions: HashMap<String, i64>,
}

impl<T> Client<T>
where
    T: serde::de::DeserializeOwned + Clone + Sync + Send + 'static,
{
    pub fn new(url: String, client_id: ClientId) -> Self {
        let (broadcast_tx, _) = broadcast::channel(256);
        Self {
            url,
            client_id,
            reqwest_client: reqwest::Client::new(),
            state: Arc::new(RwLock::new(ClientState {
                subscriptions: HashMap::new(),
            })),
            broadcast_tx,
            poller_running: Arc::new(AtomicBool::new(false)),
            state_changed: Arc::new(Notify::new()),
            _phantom: std::marker::PhantomData,
        }
    }

    pub fn get_id(&self) -> ClientId {
        self.client_id.clone()
    }

    fn ensure_poller_started(&self) {
        if self
            .poller_running
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            self.start_background_poller();
        }
    }

    pub async fn subscribe(&self, topic: &str, latest_id: i64) {
        let mut state = self.state.write().await;
        state.subscriptions.insert(topic.to_owned(), latest_id);
        drop(state);
        self.state_changed.notify_waiters();
    }

    pub async fn subscribe_all(&self, subscriptions: HashMap<&str, i64>) {
        let mut state = self.state.write().await;
        for (topic, last_message_id) in subscriptions.iter() {
            state
                .subscriptions
                .insert(topic.to_string(), *last_message_id);
        }
        self.state_changed.notify_waiters();
    }

    pub async fn unsubscribe(&self, topic: &str) {
        let mut state = self.state.write().await;
        state.subscriptions.remove(topic);
        drop(state);
        self.state_changed.notify_waiters();
    }

    pub async fn unsubscribe_all(&self, topics: Vec<&str>) {
        let mut state = self.state.write().await;
        for topic in topics {
            state.subscriptions.remove(topic);
        }
        self.state_changed.notify_waiters();
    }

    async fn update_last_message_id(state: &Arc<RwLock<ClientState>>, message: &MessageType<T>) {
        match message {
            MessageType::Status(hash_map) => {
                let mut state = state.write().await;
                for (t, i) in hash_map.iter() {
                    state
                        .subscriptions
                        .entry(t.to_owned())
                        .and_modify(|old| *old = (*old).max(*i))
                        .or_insert(*i);
                }
            }
            MessageType::Normal(message) => {
                let mut state = state.write().await;
                state
                    .subscriptions
                    .entry(message.channel.to_owned())
                    .and_modify(|old| *old = (*old).max(message.message_id))
                    .or_insert(message.message_id);
            }
        };
    }

    fn start_background_poller(&self) {
        let url = self.url.clone();
        let client_id = self.client_id;
        let reqwest_client = self.reqwest_client.clone();
        let state = Arc::clone(&self.state);
        let state_changed = Arc::clone(&self.state_changed);
        let broadcast_tx = self.broadcast_tx.clone();
        let poller_running = Arc::clone(&self.poller_running);

        tokio::spawn(async move {
            loop {
                let state_snapshot = {
                    let state = state.read().await;
                    state.subscriptions.clone()
                };

                let target_url = format!("{}{}/poll", url, client_id);
                let mut form_data = HashMap::new();

                for (topic, latest) in state_snapshot {
                    form_data.insert(topic, latest.to_string());
                }

                let response = match reqwest_client
                    .post(&target_url)
                    .form(&form_data)
                    .send()
                    .await
                {
                    Ok(r) => r,
                    Err(e) => {
                        let err = Arc::new(std::io::Error::other(e));
                        let _ = broadcast_tx.send(Err(err));
                        continue;
                    }
                };

                use futures::{StreamExt, TryStreamExt};

                let poll_stream = body_to_framed_stream(response)
                    .map(|val: Result<Value, std::io::Error>| {
                        val.and_then(|v| {
                            serde_json::from_value::<Vec<MessageType<T>>>(v).map_err(|e| {
                                std::io::Error::new(std::io::ErrorKind::InvalidData, e)
                            })
                        })
                    })
                    .map_ok(|vec| {
                        futures::stream::iter(
                            vec.into_iter().map(Ok::<MessageType<T>, std::io::Error>),
                        )
                    })
                    .try_flatten();

                tokio::pin!(poll_stream);

                loop {
                    tokio::select! {
                        msg = poll_stream.next() => {
                            match msg {
                                Some(Ok(msg_type)) => {
                                    if let MessageType::Normal(message) = msg_type.clone()
                                        && broadcast_tx.send(Ok(message)).is_err() {
                                            poller_running.store(false, Ordering::SeqCst);
                                            return;
                                        }
                                    Client::update_last_message_id(&state, &msg_type).await;

                                }
                                Some(Err(e)) => {
                                    let _ = broadcast_tx.send(Err(Arc::new(e)));
                                }
                                None => break,
                            }
                        }
                        _ = state_changed.notified() => {
                            break;
                        }
                    }
                }
            }
        });
    }

    pub fn stream(&self) -> BoxStream<'static, Result<Message<T>, Arc<std::io::Error>>> {
        self.ensure_poller_started();
        let mut rx = self.broadcast_tx.subscribe();

        let s = stream! {
            loop {
                match rx.recv().await {
                    Ok(Ok(message)) => yield Ok(message),
                    Ok(Err(err)) => yield Err(Arc::new(std::io::Error::new(err.kind(), err.to_string()))),
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        yield Err(Arc::new(std::io::Error::other(
                            format!("Stream lagged behind, {} messages dropped", n)
                        )));
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        break;
                    }
                }
            }
        };

        Box::pin(s)
    }
}
#[cfg(test)]
mod tests {
    use std::time::Duration;

    use serde_json::Value;

    use super::*;
    #[tokio::test]
    async fn client_test() -> anyhow::Result<()> {
        // let client: Client<Value> = Client::new(
        //     "https://forum.warthunder.com/message-bus/".to_owned(),
        //     ClientId::default(),
        // );
        let client = Client::builder()
            .url("https://forum.warthunder.com/message-bus/".to_owned())
            .build::<Value>()?;

        let mut stream_1 = client.stream();

        use futures::StreamExt;

        client.subscribe("/latest", -1).await;

        let handle_1 = tokio::task::spawn(async move {
            while let Some(message) = stream_1.next().await {
                println!("From 1: {:?}", message);
            }
        });

        tokio::time::sleep(Duration::from_secs(10)).await;
        let mut stream_2 = client.stream();

        let handle_2 = tokio::task::spawn(async move {
            while let Some(message) = stream_2.next().await {
                println!("From 2: {:?}", message);
            }
        });

        let _ = tokio::join!(handle_1, handle_2);

        Ok(())
    }
}
