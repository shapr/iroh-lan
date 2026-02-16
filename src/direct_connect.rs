use actor_helper::{Action, Handle, act, act_ok};
use anyhow::Result;
use iroh::{
    Endpoint, EndpointId,
    endpoint::Connection,
    protocol::ProtocolHandler,
};
use n0_watcher::Watchable;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, hash_map::Entry};
use tracing::{debug, error, info, trace};

use crate::{ConnState, Router, RouterIp, connection::Conn, local_networking::Ipv4Pkg};

const MAX_RECONNECT_ATTEMPTS: usize = 5;

#[derive(Debug, Clone)]
pub struct Direct {
    api: Handle<DirectActor, anyhow::Error>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PeerState {
    NoConnection,
    Connecting,
    Connected(Conn),
}

#[derive(Debug, Clone)]
pub struct ConnGen {
    conn: Watchable<Option<Conn>>,
    accept_conn: Watchable<PeerState>,
    open_conn: Watchable<PeerState>,
    attempts: Watchable<usize>,
}

impl Default for ConnGen {
    fn default() -> Self {
        Self {
            conn: Watchable::new(None),
            accept_conn: Watchable::new(PeerState::NoConnection),
            open_conn: Watchable::new(PeerState::NoConnection),
            attempts: Watchable::new(0),
        }
    }
}

#[derive(Debug)]
struct DirectActor {
    peers: HashMap<EndpointId, ConnGen>,
    endpoint: iroh::endpoint::Endpoint,
    rx: actor_helper::Receiver<Action<DirectActor>>,
    direct_connect_tx: tokio::sync::mpsc::Sender<DirectMessage>,
    router: Option<Router>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum DirectMessage {
    IpPacket(Ipv4Pkg),
    IDontLikeWarnings,
}

impl Direct {
    pub const ALPN: &[u8] = b"/iroh/lan-direct/1.0.2";
    pub fn new(
        endpoint: iroh::endpoint::Endpoint,
        direct_connect_tx: tokio::sync::mpsc::Sender<DirectMessage>,
    ) -> Self {
        let (api, rx) = Handle::channel();
        let mut actor = DirectActor {
            peers: HashMap::new(),
            endpoint,
            rx,
            direct_connect_tx,
            router: None,
        };
        tokio::spawn(async move { actor.run().await });
        Self { api }
    }

    pub async fn handle_connection(&self, conn: Connection) -> Result<()> {
        self.api
            .call(act!(actor => actor.handle_connection(conn)))
            .await
    }

    pub async fn route_packet(&self, to: EndpointId, pkg: DirectMessage) -> Result<()> {
        self.api
            .call(act!(actor => actor.route_packet(to, pkg)))
            .await
    }

    pub async fn ensure_connection(&self, to: EndpointId) -> Result<()> {
        self.api
            .call(act!(actor => actor.ensure_connection(to)))
            .await
    }

    pub async fn set_router(&self, router: Router) -> Result<()> {
        self.api
            .call(act_ok!(actor => async move { actor.set_router(router) }))
            .await
    }

    pub async fn get_endpoint(&self) -> iroh::endpoint::Endpoint {
        self.api
            .call(act!(actor => actor.get_endpoint()))
            .await
            .unwrap()
    }

    pub async fn get_peer_state(&self, endpoint_id: EndpointId) -> Result<ConnState> {
        self.api
            .call(act!(actor => actor.get_peer_state(endpoint_id)))
            .await
    }

    pub async fn close(&self) -> Result<()> {
        self.api.call(act!(actor => actor.close())).await
    }
}

impl DirectActor {
    async fn run(&mut self) -> Result<()> {
        let mut cleanup_interval = tokio::time::interval(std::time::Duration::from_secs(10));
        cleanup_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        let mut reconnect_interval = tokio::time::interval(std::time::Duration::from_millis(500));
        reconnect_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        debug!("DirectActor run loop started");
        loop {
            tokio::select! {
                Ok(action) = self.rx.recv_async() => {
                    action(self).await;
                }
                _ = cleanup_interval.tick() => {
                    //self.prune_closed_connections().await;
                }
                _ = reconnect_interval.tick() => {
                    self.connection_driver().await;
                }
                _ = tokio::signal::ctrl_c() => {
                     info!("Ctrl-C received, shutting down DirectActor");
                    break
                }
            }
        }
        Ok(())
    }

    /*
    async fn prune_closed_connections(&mut self) {
        let mut to_remove = Vec::new();
        for (id, conn) in &self.peers {
            if conn.get_state().await == crate::connection::ConnState::Closed {
                to_remove.push(*id);
            }
        }
        for id in to_remove {
            debug!("Removing closed connection to {}", id);
            self.peers.remove(&id);
        }
    } */

    async fn connection_driver(&mut self) {
        for (id, peer) in self.peers.clone() {
            if peer.open_conn.get() == PeerState::Connecting
                || peer.accept_conn.get() == PeerState::Connecting
                || peer.attempts.get() > MAX_RECONNECT_ATTEMPTS
            {
                continue;
            }

            match (peer.open_conn.get(), peer.accept_conn.get()) {
                (PeerState::Connected(_), PeerState::Connected(_))
                | (PeerState::Connected(_), PeerState::NoConnection)
                | (PeerState::NoConnection, PeerState::Connected(_)) => {
                    self.promote_new_conn(peer, id).await;
                }
                (PeerState::NoConnection, PeerState::NoConnection) => {
                    let open_new = if let Some(conn) = peer.conn.get()
                        && matches!(
                            conn.get_state().await.get(),
                            crate::connection::ConnState::Disconnected
                                | crate::connection::ConnState::Closed
                                | crate::connection::ConnState::ClosedAndStopped
                        ) {
                        true
                    } else {
                        peer.conn.get().is_none()
                    };

                    if open_new {
                        let attempts = peer.attempts.clone();
                        attempts.set(attempts.get() + 1).ok();
                        Self::open_new_connection(
                            id,
                            peer.clone(),
                            self.endpoint.clone(),
                            self.direct_connect_tx.clone(),
                        )
                        .await;
                    }
                }
                _ => {
                    debug!(
                        "Peer {} is in intermediate state (open={:?}, accept={:?}), skipping",
                        id,
                        peer.open_conn.get(),
                        peer.accept_conn.get()
                    );
                    continue;
                }
            }
        }
    }

    async fn open_new_connection(
        peer_id: EndpointId,
        peer: ConnGen,
        endpoint: Endpoint,
        direct_connect_tx: tokio::sync::mpsc::Sender<DirectMessage>,
    ) {
        peer.open_conn.set(PeerState::Connecting).ok();

        // Try open new connection
        tokio::spawn(async move {
            if let Ok(new_conn) =
                Conn::open_connection(endpoint.clone(), peer_id, direct_connect_tx)
                    .await
            {
                debug!("Successfully established connection to {}", peer_id);
                peer.open_conn
                    .set(PeerState::Connected(new_conn.clone()))
                    .ok();
            } else {
                error!("Failed to establish connection to {}", peer_id);
                peer.open_conn.set(PeerState::NoConnection).ok();
            }
        });
    }

    async fn promote_new_conn(&mut self, peer: ConnGen, peer_id: EndpointId) {
        debug!("Promoting new connection to {}", peer_id);
        if let Some(old_conn) = peer.conn.get() {
            debug!("Dropping old connection to {}", peer_id);
            old_conn.drop().await;
        }

        match (peer.open_conn.get(), peer.accept_conn.get()) {
            (PeerState::Connected(open), PeerState::Connected(accept)) => {
                if peer_id < self.endpoint.id() {
                    debug!("Promoting accept connection for {}", peer_id);
                    peer.conn.set(Some(accept)).ok();
                    open.drop().await;
                    peer.open_conn.set(PeerState::NoConnection).ok();
                } else {
                    debug!("Promoting open connection for {}", peer_id);
                    peer.conn.set(Some(open)).ok();
                    accept.drop().await;
                    peer.accept_conn.set(PeerState::NoConnection).ok();
                }
                peer.open_conn.set(PeerState::NoConnection).ok();
                peer.accept_conn.set(PeerState::NoConnection).ok();
                peer.attempts.set(0).ok();
            }
            (PeerState::Connected(open), PeerState::NoConnection) => {
                debug!("Promoting open connection for {}", peer_id);
                peer.conn.set(Some(open)).ok();
                peer.open_conn.set(PeerState::NoConnection).ok();
                peer.attempts.set(0).ok();
            }
            (PeerState::NoConnection, PeerState::Connected(accept)) => {
                debug!("Promoting accept connection for {}", peer_id);
                peer.conn.set(Some(accept)).ok();
                peer.accept_conn.set(PeerState::NoConnection).ok();
                peer.attempts.set(0).ok();
            }
            _ => {
                unreachable!("Invalid state during promotion for {}", peer_id);
            }
        }
    }

    async fn handle_connection(&mut self, conn: iroh::endpoint::Connection) -> Result<()> {
        info!("New direct connection from {:?}", conn.remote_id());
        let remote_id = conn.remote_id();
        if let Some(router) = &self.router {
            if !matches!(router.get_ip_state().await, Ok(RouterIp::AssignedIp(_))) {
                info!(
                    "Accepting connection from {} before local IP assignment",
                    remote_id
                );
            }

            if router.get_ip_from_endpoint_id(remote_id).await.is_err() {
                info!(
                    "Accepting connection from {} before remote IP assignment",
                    remote_id
                );
            }
        } else {
            info!(
                "Accepting connection from {} before router ready",
                remote_id
            );
        }

        let remote_id = conn.remote_id();
        let peer = self.peers.entry(remote_id).or_default().clone();
        if peer.accept_conn.get() != PeerState::NoConnection {
            error!(
                "Already have an active or pending connection with {}, rejecting new connection",
                remote_id
            );
            return Err(anyhow::anyhow!(
                "Already have an active or pending connection with {}",
                remote_id
            ));
        }
        peer.accept_conn.set(PeerState::Connecting).ok();

        if let Ok(remote_conn) =
            Conn::accept_connection(conn, self.direct_connect_tx.clone()).await
        {
            debug!("Successfully accepted connection from {}", remote_id);
            peer.accept_conn
                .set(PeerState::Connected(remote_conn.clone()))
                .ok();
            Ok(())
        } else {
            error!("Failed to accept connection from {}", remote_id);

            peer.accept_conn.set(PeerState::NoConnection).ok();
            Err(anyhow::anyhow!(
                "Failed to accept connection from {}",
                remote_id
            ))
        }
    }

    async fn route_packet(&mut self, to: EndpointId, pkg: DirectMessage) -> Result<()> {
        trace!("Routing packet to {}", to);
        match self.peers.entry(to) {
            Entry::Occupied(entry) => {
                let local_conn = if let Some(conn) = entry.get().conn.get() {
                    conn
                } else {
                    return Err(anyhow::anyhow!(
                        "Connection to {} is not currently open",
                        to
                    ));
                };
                if let Err(e) = local_conn.write(pkg).await {
                    error!("Failed to write packet to peer {}: {}", to, e);
                    return Err(e);
                }
            }
            Entry::Vacant(entry) => {
                info!("No active connection to {}, initiating new connection", to);
                entry.insert(Default::default());
            }
        }

        Ok(())
    }

    async fn ensure_connection(&mut self, to: EndpointId) -> Result<()> {
        if self.peers.contains_key(&to) {
            return Ok(());
        }

        info!(
            "No active connection to {}, initiating new connection (ensure_connection)",
            to
        );
        self.peers.insert(to, Default::default());
        Ok(())
    }

    pub async fn get_peer_state(&self, endpoint_id: EndpointId) -> Result<ConnState> {
        let peer = self
            .peers
            .get(&endpoint_id)
            .ok_or(anyhow::anyhow!("no connection to peer"))?;
        if let Some(conn) = peer.conn.get() {
            return Ok(conn.get_state().await.get());
        }
        Ok(ConnState::Closed)
    }

    pub async fn get_endpoint(&self) -> Result<iroh::endpoint::Endpoint> {
        Ok(self.endpoint.clone())
    }

    fn set_router(&mut self, router: Router) {
        self.router = Some(router);
    }

    pub async fn close(&mut self) -> Result<()> {
        for (_, conn) in self.peers.drain() {
            if let Some(conn) = conn.conn.get() {
                conn.drop().await;
            }
        }
        Ok(())
    }
}

impl ProtocolHandler for Direct {
    async fn accept(
        &self,
        connection: iroh::endpoint::Connection,
    ) -> Result<(), iroh::protocol::AcceptError> {
        let _ = self.handle_connection(connection).await;
        Ok(())
    }
}
