use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use futures::stream::BoxStream;
use serde::de::DeserializeOwned;
use thiserror::Error;
use tokio::sync::{
    Mutex, Notify, RwLock,
    mpsc::{self},
};

use crate::{
    client::ClientId,
    client::decoder::body_to_framed_stream,
    message::{Data, PollerMessage},
};
use async_stream::stream;

pub(crate) type IStreamItem<T> = Result<PollerMessage<T>, ClientError>;
pub struct Client<T>
where
    T: DeserializeOwned + Sync + Send,
{
    pub(crate) url: String,
    pub(crate) client_id: ClientId,
    pub(crate) reqwest_client: reqwest::Client,
    pub(crate) state: Arc<RwLock<ClientState>>,
    pub(crate) sender: mpsc::Sender<IStreamItem<T>>,
    pub(crate) receiver: tokio::sync::Mutex<Option<mpsc::Receiver<IStreamItem<T>>>>,
    pub(crate) poller_running: Arc<AtomicBool>,
    pub(crate) state_changed: Arc<Notify>,
    pub(crate) resume_tx: Arc<Mutex<Option<mpsc::Sender<Vec<ResumeCommand>>>>>,
    pub(crate) abort_tx: Arc<Mutex<Option<mpsc::Sender<()>>>>,
    pub(crate) pause_after_poll: bool,
    pub(crate) _phantom: std::marker::PhantomData<T>,
}

#[derive(Default)]
pub(crate) struct ClientState {
    pub(crate) subscriptions: HashMap<String, i64>,
    pub(crate) seq: i64,
}

pub enum ResumeCommand {
    Subscribe((String, i64)),
    Unsubscribe(String),
}

#[derive(Error, Debug)]
pub enum ClientError {
    #[error("no poller")]
    NoPoller,
    #[error("aborted")]
    Abort,
    #[error("io error")]
    IoError(std::io::Error),
    #[error("request error")]
    RequestError(Arc<reqwest::Error>),
    #[error("unknown erorr")]
    UnknownError,
    #[error("http error")]
    HttpError(reqwest::Error),
    #[error("incomplete stream")]
    Incomplete,
    #[error("data error")]
    DataError(serde_json::Error),
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

    pub async fn subscribe_from(&self, topic: &str, latest_id: i64) {
        let mut state = self.state.write().await;
        state.subscriptions.insert(topic.to_owned(), latest_id);
        drop(state);
        self.state_changed.notify_waiters();
    }

    pub async fn subscribe(&self, topic: &str) {
        self.subscribe_from(topic, -1).await;
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

    pub async fn abort(&self) -> Result<(), ClientError> {
        let tx = {
            let tx = self.abort_tx.lock().await;
            tx.clone()
        };
        match tx {
            Some(sender) => {
                if let Err(_) = sender.send(()).await {
                    return Err(ClientError::NoPoller);
                }
            }
            None => return Err(ClientError::NoPoller),
        }
        Ok(())
    }

    pub async fn drain(self) -> Result<Vec<(String, i64)>, ClientError> {
        self.abort().await?;
        let state: Vec<(String, i64)> = self
            .state
            .read()
            .await
            .subscriptions
            .iter()
            .map(|i| (i.0.clone(), i.1.clone()))
            .collect();

        Ok(state)
    }

    async fn prepare_request(
        state: Arc<RwLock<ClientState>>,
        client: &reqwest::Client,
        target_url: &str,
    ) -> reqwest::RequestBuilder {
        let state_snapshot = {
            let state = state.read().await;
            state.subscriptions.clone()
        };

        let mut form_data = HashMap::new();

        for (topic, latest) in state_snapshot {
            form_data.insert(topic, latest.to_string());
        }

        let seq = {
            let mut state = state.write().await;
            state.seq = state.seq + 1;
            state.seq.clone()
        };

        form_data.insert("__seq".to_string(), seq.to_string());

        client.post(target_url).form(&form_data)
    }

    fn start_background_poller(&self) {
        let url = self.url.clone();
        let client_id = self.client_id;
        let reqwest_client = self.reqwest_client.clone();

        let state = Arc::clone(&self.state);
        let state_changed = Arc::clone(&self.state_changed);
        let poller_running = Arc::clone(&self.poller_running);

        let sender = self.sender.clone();
        let resume_tx_holder = self.resume_tx.clone();
        let abort_tx_holder = self.abort_tx.clone();

        let pause_after_poll = self.pause_after_poll;

        let (resume_tx, mut resume_rx) = mpsc::channel::<Vec<ResumeCommand>>(1024);
        let (abort_tx, mut abort_rx) = mpsc::channel::<()>(1024);

        tokio::spawn(async move {
            resume_tx_holder.lock().await.replace(resume_tx);
            abort_tx_holder.lock().await.replace(abort_tx);
            let target_url = url.to_owned();
            loop {
                let post_request =
                    Client::<T>::prepare_request(state.clone(), &reqwest_client, &target_url).await;

                let response = match post_request.send().await {
                    Ok(r) => r,
                    Err(e) => {
                        let _ = sender.send(Err(ClientError::RequestError(Arc::new(e))));
                        continue;
                    }
                };

                use futures::{StreamExt, TryStreamExt};

                let poll_stream = body_to_framed_stream(response)
                    .and_then(|v| async move {
                        serde_json::from_value::<Vec<Data<T>>>(v).map_err(ClientError::DataError)
                    })
                    .map_ok(|vec| {
                        futures::stream::iter(vec.into_iter().map(Ok::<Data<T>, ClientError>))
                    })
                    .try_flatten();

                tokio::pin!(poll_stream);

                loop {
                    tokio::select! {
                        msg = poll_stream.next() => {
                            match msg {
                                Some(Ok(msg_type)) => {
                                    if let Data::Normal(message) = msg_type.clone()
                                        && sender.send(Ok(PollerMessage::UserMessage(Arc::new(message)))).await.is_err() {
                                            poller_running.store(false, Ordering::SeqCst);
                                            return;
                                        }
                                    Client::update_last_message_id(&state, &msg_type).await;

                                }
                                Some(Err(e)) => {
                                    if let Err(_) = sender.send(Err(e)).await {
                                        poller_running.store(false, Ordering::SeqCst);
                                        return;
                                    }
                                }
                                None => {
                                    if sender.send(Ok(PollerMessage::PollEnded)).await.is_err() {
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
                        _ = abort_rx.recv() => {
                            poller_running.store(false, Ordering::SeqCst);
                            let _ = sender.send(Err(ClientError::Abort));
                            return;
                        }
                    }
                }
            }
        });
    }

    pub fn stream(&mut self) -> Option<BoxStream<'static, IStreamItem<T>>> {
        self.ensure_poller_started();
        let mut rx = self.receiver.get_mut().take()?;

        let s = stream! {
            while let Some(item) = rx.recv().await {
                yield item
            }
        };

        Some(Box::pin(s))
    }
}
