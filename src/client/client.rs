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
use thiserror::Error;
use tokio::sync::{
    Mutex, Notify, RwLock,
    broadcast::{self},
    mpsc::{self},
};

use crate::{
    client::{client_id::ClientId, decoder::body_to_framed_stream},
    message::{Data, PollerMessage},
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
    pub(crate) broadcast_tx: broadcast::Sender<Result<PollerMessage<T>, Arc<std::io::Error>>>,
    pub(crate) poller_running: Arc<AtomicBool>,
    pub(crate) state_changed: Arc<Notify>,
    pub(crate) resume_tx: Arc<Mutex<Option<mpsc::Sender<Vec<ResumeCommand>>>>>,
    pub(crate) pause_after_poll: bool,
    pub(crate) _phantom: std::marker::PhantomData<T>,
}

#[derive(Default)]
pub(crate) struct ClientState {
    pub(crate) subscriptions: HashMap<String, i64>,
}

pub enum ResumeCommand {
    Subscribe((String, i64)),
    Unsubscribe(String),
}

#[derive(Error, Debug)]
pub enum ClientError {
    #[error("no poller")]
    NoPoller,
}

impl<T> Client<T>
where
    T: serde::de::DeserializeOwned + Clone + Sync + Send + 'static,
{
    pub fn get_id(&self) -> ClientId {
        self.client_id
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

    async fn update_last_message_id(state: &Arc<RwLock<ClientState>>, message: &Data<T>) {
        match message {
            Data::Status(hash_map) => {
                let mut state = state.write().await;
                for (t, i) in hash_map.iter() {
                    state
                        .subscriptions
                        .entry(t.to_owned())
                        .and_modify(|old| *old = (*old).max(*i))
                        .or_insert(*i);
                }
            }
            Data::Normal(message) => {
                let mut state = state.write().await;
                state
                    .subscriptions
                    .entry(message.channel.to_owned())
                    .and_modify(|old| *old = (*old).max(message.message_id))
                    .or_insert(message.message_id);
            }
        };
    }

    pub async fn resume_inner(&self, commands: Vec<ResumeCommand>) -> Result<(), ClientError> {
        let tx = {
            let tx = self.resume_tx.lock().await;
            tx.clone()
        };

        match tx {
            Some(sender) => {
                if let Err(_) = sender.send(commands).await {
                    return Err(ClientError::NoPoller);
                }
            }
            None => return Err(ClientError::NoPoller),
        }
        Ok(())
    }

    pub async fn resume(&self) -> Result<(), ClientError> {
        self.resume_inner(vec![]).await
    }

    pub async fn resume_with(&self, commands: Vec<ResumeCommand>) -> Result<(), ClientError> {
        self.resume_inner(commands).await
    }

    fn start_background_poller(&self) {
        let url = self.url.clone();
        let client_id = self.client_id;
        let reqwest_client = self.reqwest_client.clone();
        let state = Arc::clone(&self.state);
        let state_changed = Arc::clone(&self.state_changed);
        let broadcast_tx = self.broadcast_tx.clone();
        let poller_running = Arc::clone(&self.poller_running);
        let resume_tx_holder = self.resume_tx.clone();
        let pause_after_poll = self.pause_after_poll;

        let (resume_tx, mut resume_rx) = mpsc::channel::<Vec<ResumeCommand>>(1024);

        tokio::spawn(async move {
            resume_tx_holder.lock().await.replace(resume_tx);
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
                            serde_json::from_value::<Vec<Data<T>>>(v).map_err(|e| {
                                std::io::Error::new(std::io::ErrorKind::InvalidData, e)
                            })
                        })
                    })
                    .map_ok(|vec| {
                        futures::stream::iter(vec.into_iter().map(Ok::<Data<T>, std::io::Error>))
                    })
                    .try_flatten();

                tokio::pin!(poll_stream);

                loop {
                    tokio::select! {
                        msg = poll_stream.next() => {
                            match msg {
                                Some(Ok(msg_type)) => {
                                    if let Data::Normal(message) = msg_type.clone()
                                        && broadcast_tx.send(Ok(PollerMessage::UserMessage(message))).is_err() {
                                            poller_running.store(false, Ordering::SeqCst);
                                            return;
                                        }
                                    Client::update_last_message_id(&state, &msg_type).await;

                                }
                                Some(Err(e)) => {
                                    let _ = broadcast_tx.send(Err(Arc::new(e)));
                                }
                                None => {
                                    if broadcast_tx.send(Ok(PollerMessage::PollEnded)).is_err() {
                                        poller_running.store(false, Ordering::SeqCst);
                                        return;
                                    }
                                    if pause_after_poll {
                                        if let Some(commands) = resume_rx.recv().await {
                                            let mut state = state.write().await;
                                            for command in commands {
                                                match command {
                                                    ResumeCommand::Subscribe((channel, id)) => {
                                                        state.subscriptions.insert(channel, id);
                                                    },
                                                    ResumeCommand::Unsubscribe(channel) => {
                                                        state.subscriptions.remove(&channel);
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    break;
                                },
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

    pub fn stream(&self) -> BoxStream<'static, Result<PollerMessage<T>, Arc<std::io::Error>>> {
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
        let client = Client::builder()
            .url("https://forum.warthunder.com/message-bus/".to_owned())
            .pause_after_poll(true)
            .build::<Value>()?;

        let mut stream_1 = client.stream();

        use futures::StreamExt;

        client.subscribe("/latest", -1).await;

        let handle_1 = tokio::task::spawn(async move {
            while let Some(message) = stream_1.next().await {
                println!("{:?}", message);
            }
        });

        tokio::time::sleep(Duration::from_secs(35)).await;
        let _ = client.resume().await;

        let _ = tokio::join!(handle_1);

        Ok(())
    }
}
