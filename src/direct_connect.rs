use actor_helper::{Action, Handle, act, act_ok};
use anyhow::Result;
use iroh::{
    Endpoint, EndpointId,
    endpoint::{Connection, Side},
    protocol::ProtocolHandler,
};
use n0_watcher::Watchable;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, hash_map::Entry};
use tracing::{debug, error, info, trace};

use crate::{Router, RouterIp, connection::Conn, local_networking::Ipv4Pkg};

const MAX_RECONNECT_ATTEMPTS: usize = 5;

#[derive(Debug, Clone)]
pub struct Direct {
    api: Handle<DirectActor, anyhow::Error>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PeerState {
    NoConnection,
    Connecting,
    Connected,
    Gone,
}

#[derive(Debug, Clone)]
pub struct ConnGen {
    conn: Watchable<Option<Conn>>,
    generation: Watchable<usize>,
    state: Watchable<PeerState>,
    attempts: Watchable<usize>,
}

impl Default for ConnGen {
    fn default() -> Self {
        Self {
            conn: Watchable::new(None),
            generation: Watchable::new(usize::default()),
            state: Watchable::new(PeerState::NoConnection),
            attempts: Watchable::new(0),
        }
    }
}

#[derive(Debug)]
struct DirectActor {
    peers: HashMap<EndpointId, ConnGen>,
    endpoint: iroh::endpoint::Endpoint,
    rx: actor_helper::Receiver<Action<DirectActor>>,
    self_handle: Handle<DirectActor, anyhow::Error>,
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
            self_handle: api.clone(),
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

    pub async fn get_peer_state(&self, endpoint_id: EndpointId) -> Result<PeerState> {
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
            // skip if:
            // - peer.state is Connecting | Gone
            if matches!(peer.state.get(), PeerState::Connecting | PeerState::Gone) {
                continue;
            }
            // - peer.state is Connected and peer.conn.state is Open | Connecting
            if matches!(peer.state.get(), PeerState::Connected)
                && let Some(conn) = peer.conn.get()
                && matches!(
                    conn.get_state().await.get(),
                    crate::connection::ConnState::Open | crate::connection::ConnState::Connecting
                )
            {
                continue;
            }

            // if peer.attempts above MAX_RECONNECT_ATTEMPTS:
            // peer.state = Gone
            // continue

            // open new if:
            // - peer.state is NoConnection
            // - peer.conn is None
            let open_new = if matches!(peer.state.get(), PeerState::NoConnection)
                || peer.conn.get().is_none()
            {
                true
            }
            // - peer.conn.state is Disconnected | Closed | ClosedAndStopped
            else if let Some(conn) = peer.conn.get()
                && matches!(
                    conn.get_state().await.get(),
                    crate::connection::ConnState::Disconnected
                        | crate::connection::ConnState::Closed
                        | crate::connection::ConnState::ClosedAndStopped
                )
            {
                true
            } else {
                false
            };

            if open_new {
                let attempts = peer.attempts.clone();
                if attempts.get() > MAX_RECONNECT_ATTEMPTS {
                    info!(
                        "Peer {} marked as Gone after {} failed attempts",
                        id, MAX_RECONNECT_ATTEMPTS
                    );
                    peer.state.set(PeerState::Gone).ok();
                    continue;
                }
                Self::open_new_connection(
                    id,
                    peer,
                    self.endpoint.clone(),
                    self.self_handle.clone(),
                    self.direct_connect_tx.clone(),
                )
                .await;
                let next_attempt = attempts.get() + 1;
                attempts.set(next_attempt).ok();
            }
        }
    }

    async fn open_new_connection(
        peer_id: EndpointId,
        peer: ConnGen,
        endpoint: Endpoint,
        handle: Handle<DirectActor, anyhow::Error>,
        direct_connect_tx: tokio::sync::mpsc::Sender<DirectMessage>,
    ) {
        peer.state.set(PeerState::Connecting).ok();
        peer.generation.set(peer.generation.get() + 1).ok();

        // Try open new connection
        tokio::spawn(async move {
            if let Ok(new_conn) = Conn::open_connection(
                endpoint.clone(),
                peer_id,
                endpoint.id(),
                direct_connect_tx,
                Some(peer.generation.get()),
            )
            .await
            {
                debug!("Successfully established connection to {}", peer_id);
                handle
                    .call(act_ok!(actor => actor.replace_connection(
                        peer.clone(),
                        peer_id,
                        endpoint.id(),
                        Side::Client,
                        new_conn
                    ))).await.ok();
            } else {
                error!("Failed to establish connection to {}", peer_id);
                handle
                    .call(act_ok!(actor => actor.replace_connection_failed(peer.clone(), peer_id, None))).await.ok();
            }
        });
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
        peer.state.set(PeerState::Connecting).ok();

        if let Ok(remote_conn) = Conn::accept_connection(
            conn,
            self.direct_connect_tx.clone(),
            Some(peer.generation.get()),
            self.endpoint.id(),
        )
        .await
        {
            self.replace_connection(
                peer,
                remote_id,
                self.endpoint.id(),
                Side::Server,
                remote_conn,
            )
            .await;
            Ok(())
        } else {
            error!("Failed to accept connection from {}", remote_id);

            self.replace_connection_failed(peer, remote_id, None).await;

            Err(anyhow::anyhow!(
                "Failed to accept connection from {}",
                remote_id
            ))
        }
    }

    async fn replace_connection(
        &mut self,
        peer: ConnGen,
        remote_id: EndpointId,
        local_id: EndpointId,
        side: Side,
        new_conn: Conn,
    ) {
        let c_generation = peer.generation.get();
        let new_generation = new_conn.get_generation().await;

        let mut keep_conditions = new_generation < c_generation;
        let has_live_connection = matches!(peer.state.get(), PeerState::Connected)
            && peer.conn.get().is_some()
            && matches!(
                peer.conn.get().unwrap().get_state().await.get(),
                crate::connection::ConnState::Open
            );

        keep_conditions |= new_generation == c_generation
             && local_id < remote_id
             && side == Side::Client
             && has_live_connection;

        keep_conditions |= new_generation == c_generation
             && local_id > remote_id
             && side == Side::Server
             && has_live_connection;

        if keep_conditions {
            debug!(
                "Rejecting new connection from {} to {} with older generation {} (current={})",
                if side == Side::Client {
                    local_id
                } else {
                    remote_id
                },
                if side == Side::Client {
                    remote_id
                } else {
                    local_id
                },
                new_generation,
                c_generation
            );
            self.replace_connection_failed(peer, remote_id, Some(new_conn)).await;
            return;
        }

        let old_conn = peer.conn.get();        
        peer.conn.set(Some(new_conn)).ok();
        if let Some(conn) = old_conn {
            conn.drop().await;
        }
        peer.generation.set(new_generation).ok();
        peer.state.set(PeerState::Connected).ok();
        peer.attempts.set(0).ok();
    }

    async fn replace_connection_failed(
        &mut self,
        peer: ConnGen,
        remote_id: EndpointId,
        new_conn: Option<Conn>,
    ) {
        if let Some(conn) = new_conn {
            conn.drop().await;
        }
        if let Some(old_conn) = peer.conn.get()
            && matches!(
                old_conn.get_state().await.get(),
                crate::connection::ConnState::Open | crate::connection::ConnState::Connecting
            )
        {
            peer.state.set(PeerState::Connected).ok();
            peer.attempts.set(0).ok();
            error!(
                "Keeping existing connection to {} despite failed accept of new connection",
                remote_id
            );
        } else {
            if let Some(old_conn) = peer.conn.get() {
                old_conn.drop().await;
            }
            peer.state.set(PeerState::NoConnection).ok();
            peer.conn.set(None).ok();
            error!(
                "No existing connection to {}. Marking as NoConnection",
                remote_id
            );
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

    pub async fn get_peer_state(&self, endpoint_id: EndpointId) -> Result<PeerState> {
        Ok(self
            .peers
            .get(&endpoint_id)
            .ok_or(anyhow::anyhow!("no connection to peer"))?
            .state
            .get())
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
