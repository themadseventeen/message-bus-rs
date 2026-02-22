use std::{
    collections::HashMap,
    sync::{Arc, atomic::AtomicBool},
};

use serde::de::DeserializeOwned;
use thiserror::Error;
use tokio::sync::{Mutex, Notify, RwLock, broadcast};

use crate::client::client::ClientState;
use crate::client::{Client, ClientId};

#[derive(Default)]
pub struct ClientBuilder {
    url: Option<String>,
    client_id: Option<ClientId>,
    reqwest_client: Option<reqwest::Client>,
    pause_after_poll: bool,
}

impl Client<()> {
    pub fn builder() -> ClientBuilder {
        ClientBuilder::default()
    }
}

impl ClientBuilder {
    pub fn url(mut self, url: impl Into<String>) -> ClientBuilder {
        self.url = Some(url.into());
        self
    }

    pub fn client_id(mut self, client_id: ClientId) -> ClientBuilder {
        self.client_id = Some(client_id);
        self
    }

    pub fn http_client(mut self, http: reqwest::Client) -> ClientBuilder {
        self.reqwest_client = Some(http);
        self
    }

    pub fn pause_after_poll(mut self, pause_after_poll: bool) -> ClientBuilder {
        self.pause_after_poll = pause_after_poll;
        self
    }

    pub fn build<T>(self) -> Result<Client<T>, BuilderError>
    where
        T: DeserializeOwned + Clone + Send + Sync,
    {
        let url = self.url.ok_or(BuilderError::UrlMissing)?;
        let (broadcast_tx, _) = broadcast::channel(256);

        Ok(Client {
            url,
            client_id: match self.client_id {
                Some(id) => id,
                None => ClientId::default(),
            },
            reqwest_client: match self.reqwest_client {
                Some(http) => http,
                None => reqwest::Client::default(),
            },
            state: Arc::new(RwLock::new(ClientState::default())),
            broadcast_tx,
            poller_running: Arc::new(AtomicBool::new(false)),
            state_changed: Arc::new(Notify::new()),
            pause_after_poll: self.pause_after_poll,
            resume_tx: Arc::new(Mutex::new(None)),
            abort_tx: Arc::new(Mutex::new(None)),
            _phantom: std::marker::PhantomData,
        })
    }
}

#[derive(Error, Debug)]
pub enum BuilderError {
    #[error("url required")]
    UrlMissing,
}
