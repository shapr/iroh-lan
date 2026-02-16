use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::{collections::VecDeque, sync::atomic::AtomicUsize, time::Duration};

use crate::DirectMessage;
use actor_helper::{Action, Actor, Handle, Receiver, act, act_ok};
use anyhow::Result;
use iroh::endpoint::{Connection, VarInt};
use iroh::{
    Endpoint, EndpointId,
    endpoint::{RecvStream, SendStream},
};
use n0_watcher::Watchable;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::{self};
use tracing::{debug, info, trace, warn};

const QUEUE_SIZE: usize = 1024 * 16;
const BACKPRESSURE_WARN_MS: u128 = 5;
const MAX_SENDER_QUEUE: usize = 50_000;
const WRITE_CHANNEL_CAP: usize = 8_192;
const STATS_LOG_INTERVAL: Duration = Duration::from_secs(5);
const QUEUE_WARN_LEN: usize = 10_000;
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(5);
const CONNECTING_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Conn {
    api: Handle<ConnActor, anyhow::Error>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnState {
    Connecting, // ConnActor::connect() called, waiting for connection to be established (in background)
    Open,       // open bi directional streams
    Closed,     // connection closed by user or error
    Disconnected, // connection closed by remote peer, can be recovered within 5 retries after Closed
    ClosedAndStopped, // connection closed and actor stopped, no recovery possible
}

#[derive(Debug)]
struct ConnActor {
    rx: Receiver<Action<ConnActor>>,
    self_handle: Handle<ConnActor, anyhow::Error>,
    state: Watchable<ConnState>,
    local_endpoint_id: EndpointId,

    // all of these need to be optionals so that we can create an empty
    // shell of the actor and then fill in the values later so we don't wait
    // forever in the main standalone loop for router events hanging on
    // route_packet failed
    conn: Option<Connection>,
    conn_endpoint_id: EndpointId,

    external_sender: tokio::sync::mpsc::Sender<DirectMessage>,

    write_task: Option<tokio::task::JoinHandle<()>>,
    write_tx: Option<tokio::sync::mpsc::Sender<DirectMessage>>,

    read_task: Option<tokio::task::JoinHandle<()>>,
    closed_task: Option<tokio::task::JoinHandle<()>>,

    queue_len: Arc<std::sync::atomic::AtomicUsize>,
    dropped_packets: Arc<AtomicUsize>,

    sender_queue: VecDeque<DirectMessage>,
    rx_count: Arc<AtomicUsize>,
    tx_count: Arc<AtomicUsize>,
    write_timeouts: Arc<AtomicUsize>,
    consecutive_write_errors: Arc<AtomicUsize>,
    conn_generation: usize,
    remote_generation: Watchable<Option<usize>>,
}

impl Conn {
    #[allow(clippy::too_many_arguments)]
    pub async fn accept_connection(
        conn: iroh::endpoint::Connection,
        external_sender: tokio::sync::mpsc::Sender<DirectMessage>,
        generation: Option<usize>,
        local_endpoint_id: EndpointId,
    ) -> Result<Self> {
        info!("[CONN] Accepting incoming gen: {generation:?}");
        let (api, rx) = Handle::channel();
        let mut actor = ConnActor::new(
            rx,
            api.clone(),
            external_sender,
            conn.remote_id(),
            local_endpoint_id,
            generation,
        )
        .await;

        let s = Self { api };

        tokio::spawn(async move {
            actor.state.set(ConnState::Connecting).ok();
            actor.run().await
        });

        let remote_id = conn.remote_id();
        let connect_fut = async {
            let (send, recv) = conn.accept_bi().await?;
            Ok::<(Connection, SendStream, RecvStream), anyhow::Error>((conn, send, recv))
        };

        match tokio::time::timeout(CONNECTING_TIMEOUT, connect_fut).await {
            Ok(Ok((conn, send, recv))) => {
                if let Err(err) = s.establish_connection(conn, send, recv).await {
                    warn!("Failed to establish connection with {}: {}", remote_id, err);
                    s.drop().await;
                    anyhow::bail!("Failed to establish connection: {}", err);
                }
            }
            Ok(Err(e)) => {
                warn!("Initial connection to {} failed: {}", remote_id, e);
                s.drop().await;
                anyhow::bail!("Failed to establish connection: {}", e);
            }
            Err(_) => {
                warn!(
                    "Initial connection to {} timed out after {}s",
                    remote_id,
                    CONNECTING_TIMEOUT.as_secs()
                );
                s.drop().await;
                anyhow::bail!("Connection timed out");
            }
        }

        Ok(s)
    }

    pub async fn open_connection(
        endpoint: Endpoint,
        remote_endpoint_id: EndpointId,
        local_endpoint_id: EndpointId,
        external_sender: tokio::sync::mpsc::Sender<DirectMessage>,
        generation: Option<usize>,

    ) -> Result<Self> {
        info!("[CONN] Opening incoming gen: {generation:?}");
        let (api, rx) = Handle::channel();
        let mut actor =
            ConnActor::new(rx, api.clone(), external_sender, remote_endpoint_id, local_endpoint_id, generation).await;

        tokio::spawn(async move {
            actor.state.set(ConnState::Connecting).ok();
            actor.run().await
        });
        let s = Self { api };

        let connect_fut = async {
            let conn = endpoint.connect(remote_endpoint_id, crate::Direct::ALPN).await?;
            let (send, recv) = conn.open_bi().await?;
            Ok::<(Connection, SendStream, RecvStream), anyhow::Error>((conn, send, recv))
        };

        match tokio::time::timeout(CONNECTING_TIMEOUT, connect_fut).await {
            Ok(Ok((conn, send, recv))) => {
                if let Err(err) = s.establish_connection(conn, send, recv).await {
                    warn!("Failed to establish connection with {}: {}", remote_endpoint_id, err);
                    s.drop().await;
                    anyhow::bail!("Failed to establish connection: {}", err);
                }
            }
            Ok(Err(e)) => {
                warn!("Initial connection to {} failed: {}", remote_endpoint_id, e);
                s.drop().await;
                anyhow::bail!("Failed to establish connection: {}", e);
            }
            Err(_) => {
                warn!(
                    "Initial connection to {} timed out after {}s",
                    remote_endpoint_id,
                    CONNECTING_TIMEOUT.as_secs()
                );
                s.drop().await;
                anyhow::bail!("Connection timed out");
            }
        }

        Ok(s)
    }

    pub async fn get_state(&self) -> Watchable<ConnState> {
        if let Ok(state) = self
            .api
            .call(act_ok!(actor => async move {
                actor.state.clone()
            }))
            .await
        {
            state
        } else {
            Watchable::new(ConnState::ClosedAndStopped)
        }
    }

    /*
    pub async fn get_conn(&self) -> Option<Connection> {
        self.api
            .call(act_ok!(actor => async {
                actor.conn.clone()
            }))
            .await
            .unwrap_or_default()
    } */

    pub async fn write(&self, pkg: DirectMessage) -> Result<()> {
        self.api.call(act_ok!(actor => actor.write(pkg))).await
    }

    pub async fn establish_connection(
        &self,
        conn: Connection,
        send_stream: SendStream,
        recv_stream: RecvStream,
    ) -> Result<()> {
        self.api
            .call(act!(actor => actor.establish_connection(conn, send_stream, recv_stream)))
            .await
    }

    pub async fn get_generation(&self) -> usize {
        self.api
            .call(act_ok!(actor => async {
                actor.conn_generation
            }))
            .await
            .unwrap_or_default()
    }

    pub async fn get_remote_generation(&self) -> Option<usize> {
        self.api
            .call(act_ok!(actor => async {
                actor.remote_generation.get()
            }))
            .await
            .unwrap_or_default()
    }

    pub async fn drop(&self) {
        self.api.call(act_ok!(actor => async {
                actor.close().await;
                actor.state.set(ConnState::ClosedAndStopped).ok();
            }))
            .await
            .ok();
    }
}

impl Actor<anyhow::Error> for ConnActor {
    async fn run(&mut self) -> Result<()> {
        //let mut reconnect_ticker = tokio::time::interval(Duration::from_millis(500));
        //reconnect_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        let mut keepalive_ticker = tokio::time::interval(KEEPALIVE_INTERVAL);
        keepalive_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        let mut stats_ticker = tokio::time::interval(STATS_LOG_INTERVAL);
        stats_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        debug!("ConnActor started for peer: {}", self.conn_endpoint_id);

        loop {
            if self.state.get() == ConnState::ClosedAndStopped {
                debug!(
                    "ConnActor for {} is in ClosedAndStopped state, exiting run loop",
                    self.conn_endpoint_id
                );
                break;
            }
            tokio::select! {
                Ok(action) = self.rx.recv_async() => {
                    action(self).await;
                }
                /*_ = reconnect_ticker.tick(), if self.state != ConnState::Closed => {

                    let need_reconnect = !matches!(self.state, ConnState::Open | ConnState::Connecting) &&
                        (self.write_task.as_ref().map(|t| t.is_finished()).unwrap_or(false)
                        || self.read_task.as_ref().map(|t| t.is_finished()).unwrap_or(false)
                        || self.conn.as_ref().and_then(|c| c.close_reason()).is_some()
                        || self.closed_task.as_ref().map(|t| t.is_finished()).unwrap_or(false)
                    );

                    if need_reconnect && self.last_reconnect.elapsed() > self.reconnect_backoff {
                        if self.reconnect_count.load(Ordering::SeqCst) < MAX_RECONNECTS {
                            warn!("Write task finished or connection issues detected. Attempting reconnect.");
                            let _ = self.try_reconnect().await;
                        } else {
                            warn!("Max reconnects reached, closing connection to {}", self.conn_endpoint_id);
                            break;
                        }
                    }
                }*/
                _ = keepalive_ticker.tick(), if self.state.get() == ConnState::Open => {
                    if let Some(tx) = &self.write_tx {
                        match tx.try_send(DirectMessage::IDontLikeWarnings) {
                            Ok(_) => {
                                let new_len = self.queue_len.fetch_add(1, Ordering::Relaxed) + 1;
                                if new_len > QUEUE_WARN_LEN {
                                    warn!("Stream queue length high (keepalive): {}", new_len);
                                }
                            }
                            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                                self.dropped_packets.fetch_add(1, Ordering::Relaxed);
                            }
                            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                                self.dropped_packets.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                }
                _ = stats_ticker.tick() => {
                    let q_len = self.queue_len.load(Ordering::Relaxed);
                    if q_len > 100 {
                        warn!("[PROBE-QUEUE] High Queue Len: {}", q_len);
                    }
                    debug!(
                        "Conn stats: endpoint_id={} state={:?} rx_count={} tx_count={} queue_len={} write_timeouts={} write_errors={} dropped_packets={}",
                        self.conn_endpoint_id,
                        self.state.get(),
                        self.rx_count.load(Ordering::Relaxed),
                        self.tx_count.load(Ordering::Relaxed),
                        self.queue_len.load(Ordering::Relaxed),
                        self.write_timeouts.load(Ordering::Relaxed),
                        self.consecutive_write_errors.load(Ordering::Relaxed),
                        self.dropped_packets.load(Ordering::Relaxed)
                    );
                }
                _ = tokio::signal::ctrl_c() => {
                    info!("Received Ctrl-C, stopping actor");
                    break
                }
            }
        }
        Ok(())
    }
}

impl ConnActor {
    fn spawn_closed_task(
        api: Handle<ConnActor, anyhow::Error>,
        conn: Connection,
        generation: usize,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let _reason = conn.closed().await;
            let _ = api
                .call(act_ok!(actor => async move {
                    actor.handle_connection_closed(generation).await;
                }))
                .await;
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        rx: Receiver<Action<ConnActor>>,
        self_handle: Handle<ConnActor, anyhow::Error>,
        external_sender: tokio::sync::mpsc::Sender<DirectMessage>,
        conn_endpoint_id: EndpointId,
        local_endpoint_id: EndpointId,
        generation: Option<usize>,
    ) -> Self {
        Self {
            rx,
            state: Watchable::new(ConnState::Connecting),
            external_sender,
            local_endpoint_id,
            read_task: None,
            write_task: None,
            closed_task: None,
            write_tx: None,
            queue_len: Arc::new(AtomicUsize::new(0)),
            sender_queue: VecDeque::with_capacity(QUEUE_SIZE),
            conn: None,
            conn_endpoint_id,
            self_handle,
            rx_count: Arc::new(AtomicUsize::new(0)),
            tx_count: Arc::new(AtomicUsize::new(0)),
            write_timeouts: Arc::new(AtomicUsize::new(0)),
            consecutive_write_errors: Arc::new(AtomicUsize::new(0)),
            dropped_packets: Arc::new(AtomicUsize::new(0)),
            conn_generation: generation.unwrap_or_default(),
            remote_generation: Watchable::new(None),
        }
    }

    pub async fn close(&mut self) {
        info!("Closing connection actor");
        if let Some(conn) = self.conn.as_mut() {
            conn.close(VarInt::from_u32(400), b"Connection closed by user");
        }
        if let Some(task) = self.read_task.take() {
            task.abort();
        }
        if let Some(task) = self.write_task.take() {
            task.abort();
        }
        if let Some(task) = self.closed_task.take() {
            task.abort();
        }
        self.write_tx = None;
        self.conn = None;
        self.state.set(ConnState::Closed).ok();
    }

    pub async fn handle_connection_closed(&mut self, generation: usize) {
        if generation != self.conn_generation {
            trace!(
                "Ignoring stale closed event for generation {}, current generation {}",
                generation, self.conn_generation
            );
            return;
        }

        if self.state.get() == ConnState::Closed {
            return;
        }

        warn!(
            "Connection closed event received for {} (generation={})",
            self.conn_endpoint_id, generation
        );
        self.write_tx = None;
        if let Some(task) = self.write_task.take() {
            task.abort();
        }
        if let Some(task) = self.read_task.take() {
            task.abort();
        }
        if !matches!(self.state.get(), ConnState::Disconnected | ConnState::ClosedAndStopped | ConnState::Closed) {
            self.state.set(ConnState::Disconnected).ok();
        }
    }

    pub async fn handle_write_error(&mut self) {
        self.consecutive_write_errors
            .fetch_add(1, Ordering::Relaxed);
        warn!(
            "Write loop failed. consecutive_write_errors={}",
            self.consecutive_write_errors.load(Ordering::Relaxed)
        );
        self.write_tx = None;
        if let Some(task) = self.write_task.take() {
            task.abort();
        }
        if !matches!(self.state.get(), ConnState::Disconnected | ConnState::ClosedAndStopped | ConnState::Closed) {
            self.state.set(ConnState::Disconnected).ok();
        }
    }

    pub async fn write(&mut self, pkg: DirectMessage) {
        if let Some(tx) = &self.write_tx {
            trace!("Sending packet to write task");
            match tx.try_send(pkg) {
                Ok(_) => {
                    let new_len = self.queue_len.fetch_add(1, Ordering::Relaxed) + 1;
                    if new_len > QUEUE_WARN_LEN {
                        warn!("Stream queue length high: {}", new_len);
                    }
                }
                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                    self.dropped_packets.fetch_add(1, Ordering::Relaxed);
                    if self
                        .dropped_packets
                        .load(Ordering::Relaxed)
                        .is_multiple_of(1000)
                    {
                        warn!(
                            "Write queue full, dropping packet (dropped={})",
                            self.dropped_packets.load(Ordering::Relaxed)
                        );
                    }
                }
                Err(tokio::sync::mpsc::error::TrySendError::Closed(val)) => {
                    warn!("Write task channel closed, buffering packet.");
                    self.sender_queue.push_front(val);
                    while self.sender_queue.len() > MAX_SENDER_QUEUE {
                        self.sender_queue.pop_back();
                    }
                    if self.state.get() == ConnState::Open {
                        self.state.set(ConnState::Disconnected).ok();
                    }
                }
            }
        } else {
            trace!(
                "Queueing packet for write. Queue size: {}",
                self.sender_queue.len()
            );
            self.sender_queue.push_front(pkg);
            while self.sender_queue.len() > MAX_SENDER_QUEUE {
                self.sender_queue.pop_back();
            }
        }
    }

    pub async fn establish_connection(
        &mut self,
        conn: Connection,
        mut send_stream: SendStream,
        mut recv_stream: RecvStream,
    ) -> Result<()> {
        info!("Incoming connection from: {}", conn.remote_id());
        if conn.close_reason().is_some() {
            warn!("Incoming connection already closed");
            self.state.set(ConnState::Disconnected).ok();
            return Err(anyhow::anyhow!("connection closed"));
        }

        // Nagotiate generation, only accept if higher, if equal decide based on endpoint ID to avoid races
        let remote_generation = match conn.side() {
            iroh::endpoint::Side::Client => {
                if send_stream
                    .write_u64(self.conn_generation as u64)
                    .await
                    .is_err()
                {
                    self.state.set(ConnState::Closed).ok();
                    return Err(anyhow::anyhow!("failed to write generation to stream"));
                }
                let remote_generation = if let Ok(generation) = recv_stream.read_u64().await {
                    generation as usize
                } else {
                    self.state.set(ConnState::Closed).ok();
                    return Err(anyhow::anyhow!("failed to read generation from stream"));
                };
                if remote_generation > self.conn_generation {
                    self.state.set(ConnState::Closed).ok();
                    warn!("[CONN] Rejecting connection with higher generation: remote_generation={}, local_generation={}", remote_generation, self.conn_generation);
                    return Err(anyhow::anyhow!("remote has higher generation, rejecting connection"));
                }
                remote_generation
            }
            iroh::endpoint::Side::Server => {
                let remote_generation = if let Ok(generation) = recv_stream.read_u64().await {
                    generation as usize
                } else {
                    self.state.set(ConnState::Closed).ok();
                    return Err(anyhow::anyhow!("failed to read generation from stream"));
                };

                if send_stream
                    .write_u64(self.conn_generation as u64)
                    .await
                    .is_err()
                {
                    self.state.set(ConnState::Closed).ok();
                    return Err(anyhow::anyhow!("failed to write generation to stream"));
                }
                if self.conn_generation > remote_generation {
                    self.state.set(ConnState::Closed).ok();
                    warn!("[CONN] Rejecting connection with lower generation: remote_generation={}, local_generation={}", remote_generation, self.conn_generation);
                    return Err(anyhow::anyhow!("remote has lower generation, rejecting connection"));
                }
                remote_generation
            }
        };

        self.remote_generation.set(Some(remote_generation)).ok();
        self.conn_generation = remote_generation.max(self.conn_generation);

        if let Some(task) = self.read_task.take() {
            task.abort();
        }

        info!("Spawning read task for incoming connection");
        let rx_count = self.rx_count.clone();
        self.read_task = Some(tokio::spawn(retry_read_loop(
            recv_stream,
            self.external_sender.clone(),
            self.self_handle.clone(),
            rx_count,
        )));

        if let Some(task) = self.write_task.take() {
            task.abort();
        }
        if let Some(task) = self.closed_task.take() {
            task.abort();
        }

        info!("Spawning write task for incoming connection");
        let (tx, rx) = tokio::sync::mpsc::channel(WRITE_CHANNEL_CAP);
        self.queue_len.store(0, Ordering::Relaxed);
        let write_timeouts = self.write_timeouts.clone();
        let tx_count = self.tx_count.clone();
        self.write_task = Some(tokio::spawn(write_loop_bounded(
            send_stream,
            rx,
            self.self_handle.clone(),
            self.queue_len.clone(),
            "main",
            tx_count,
            write_timeouts,
        )));
        self.write_tx = Some(tx.clone());

        self.closed_task = Some(Self::spawn_closed_task(
            self.self_handle.clone(),
            conn.clone(),
            self.conn_generation,
        ));
        self.conn = Some(conn);
        self.consecutive_write_errors.store(0, Ordering::Relaxed);
        self.rx_count.store(0, Ordering::Relaxed);
        self.tx_count.store(0, Ordering::Relaxed);

        while let Some(msg) = self.sender_queue.pop_back() {
            match tx.try_send(msg) {
                Ok(_) => {
                    let new_len = self.queue_len.fetch_add(1, Ordering::Relaxed) + 1;
                    if new_len > QUEUE_WARN_LEN {
                        warn!("Stream queue length high (flush): {}", new_len);
                    }
                }
                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                    self.dropped_packets.fetch_add(1, Ordering::Relaxed);
                }
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                    self.dropped_packets.fetch_add(1, Ordering::Relaxed);
                }
            }
        }

        self.state.set(ConnState::Open).ok();

        Ok(())
    }

    /*
    async fn try_reconnect(&mut self) -> Result<()> {
        info!("Trying to reconnect to {}", self.conn_endpoint_id);
        if self.state == ConnState::Closed {
            warn!("Cannot reconnect, actor is closed");
            return Err(anyhow::anyhow!("actor closed for good"));
        }
        if self.endpoint.id() < self.conn_endpoint_id && !matches!(self.state, ConnState::Connecting) {
            debug!("Skipping reconnect attempt due to endpoint ID ordering");
            return Ok(());
        }
        self.state = ConnState::Connecting;

        if let Some(task) = self.read_task.take() {
            task.abort();
        }
        if let Some(task) = self.write_task.take() {
            task.abort();
        }
        if let Some(task) = self.closed_task.take() {
            task.abort();
        }
        self.write_tx = None;
        self.state = ConnState::Connecting;
        let next_backoff = self.reconnect_backoff * 2;
        self.reconnect_backoff = if next_backoff > RECONNECT_BACKOFF_MAX {
            RECONNECT_BACKOFF_MAX
        } else {
            next_backoff
        };
        self.last_reconnect = tokio::time::Instant::now();

        self.conn = None;

        tokio::spawn({
            let api = self.self_handle.clone();
            let endpoint = self.endpoint.clone();
            let conn_node_id = self.conn_endpoint_id;
            async move {
                debug!("Initiating reconnection to {}", conn_node_id);
                let connect_fut = async {
                    let conn = endpoint.connect(conn_node_id, crate::Direct::ALPN).await?;
                    let (send, recv) = conn.open_bi().await?;
                    Ok::<(Connection, SendStream, RecvStream), anyhow::Error>((conn, send, recv))
                };

                match tokio::time::timeout(CONNECTING_TIMEOUT, connect_fut).await {
                    Ok(Ok((conn, send, recv))) => {
                        debug!("Reconnection successful");
                        let _ = api
                            .call(act!(actor => actor.incoming_connection(conn, send, recv)))
                            .await;
                        let _ = api
                            .call(act_ok!(actor => async move { actor.reconnect_count.store(0, Ordering::SeqCst) }))
                            .await;
                    }
                    Ok(Err(e)) => {
                        warn!("Reconnection to {} failed: {}", conn_node_id, e);
                        let _ = api
                            .call(act_ok!(actor => async move { actor.reconnect_count.fetch_add(1, Ordering::SeqCst) }))
                            .await;
                        let _ = api
                            .call(
                                act_ok!(actor => async move { actor.set_state(ConnState::Disconnected) }),
                            )
                            .await;
                    }
                    Err(_) => {
                        warn!("Reconnection to {} timed out after {}s", conn_node_id, CONNECTING_TIMEOUT.as_secs());
                        let _ = api
                            .call(act_ok!(actor => async move { actor.reconnect_count.fetch_add(1, Ordering::SeqCst) }))
                            .await;
                        let _ = api
                            .call(
                                act_ok!(actor => async move { actor.set_state(ConnState::Disconnected) }),
                            )
                            .await;
                    }
                }
            }
        });
        Ok(())
    }*/
}

async fn write_loop_bounded(
    mut stream: SendStream,
    mut rx: tokio::sync::mpsc::Receiver<DirectMessage>,
    api: Handle<ConnActor, anyhow::Error>,
    queue_len: Arc<std::sync::atomic::AtomicUsize>,
    label: &'static str,
    tx_count: Arc<AtomicUsize>,
    write_timeout: Arc<AtomicUsize>,
) {
    info!("Write task started ({})", label);
    while let Some(msg) = rx.recv().await {
        let _ = queue_len.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
            Some(v.saturating_sub(1))
        });
        let bytes = match postcard::to_stdvec(&msg) {
            Ok(b) => b,
            Err(e) => {
                warn!("Failed to serialize message: {}", e);
                continue;
            }
        };

        // Coalesce length and body into a single write to avoid small packets and syscall overhead
        let mut frame = Vec::with_capacity(2 + bytes.len());
        frame.extend_from_slice(&(bytes.len() as u16).to_le_bytes());
        frame.extend_from_slice(&bytes);

        match time::timeout(KEEPALIVE_INTERVAL + Duration::from_secs(2), stream.write_all(&frame)).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                warn!("Write error (frame): {}", e);
                let _ = api.call(act_ok!(actor => actor.handle_write_error())).await;
                break;
            }
            Err(_) => {
                warn!("Write timeout (frame) ({})", label);
                write_timeout.fetch_add(1, Ordering::SeqCst);
                let _ = api.call(act_ok!(actor => actor.handle_write_error())).await;
                break;
            }
        }

        tx_count.fetch_add(1, Ordering::SeqCst);
    }
    info!("Write task stopped ({})", label);
}

async fn retry_read_loop(
    mut stream: RecvStream,
    sender: tokio::sync::mpsc::Sender<DirectMessage>,
    api: Handle<ConnActor, anyhow::Error>,
    rx_count: Arc<AtomicUsize>,
) {
    info!("Read task started");
    loop {
        match tokio::time::timeout(KEEPALIVE_INTERVAL + Duration::from_secs(2), read_next_msg(&mut stream)).await {
            Ok(Ok(msg)) => {
                rx_count.fetch_add(1, Ordering::SeqCst);
                trace!("Read message from stream, forwarding to network actor");
                let start = std::time::Instant::now();
                if let Err(e) = sender.send(msg).await {
                    warn!("Failed to forward message to network actor: {}", e);
                    break;
                }
                if start.elapsed().as_millis() > BACKPRESSURE_WARN_MS {
                    warn!(
                        "Direct->Network backpressure: send blocked {} ms",
                        start.elapsed().as_millis()
                    );
                }
            }
            Ok(Err(e)) => {
                warn!("Stream read error: {}", e);
                let _ = api
                    .call(act_ok!(actor => async move { 
                        if !matches!(actor.state.get(), ConnState::Disconnected | ConnState::ClosedAndStopped | ConnState::Closed) {
                            actor.state.set(ConnState::Disconnected).ok();
                        }
                    }))
                    .await;
                break;
            }
            Err(e) => {
                warn!("Stream read error: timeout after {}", e);
        
                let _ = api
                    .call(act_ok!(actor => async move { 
                        if !matches!(actor.state.get(), ConnState::Disconnected | ConnState::ClosedAndStopped | ConnState::Closed) {
                            actor.state.set(ConnState::Disconnected).ok();
                        }
                    }))
                    .await;
                break;
            }
        }
    }
    info!("Read task stopped");
}

async fn read_next_msg(stream: &mut RecvStream) -> Result<DirectMessage> {
    let len = stream.read_u16_le().await?;
    let mut buf = vec![0; len as usize];
    stream.read_exact(&mut buf).await?;
    let msg: DirectMessage = postcard::from_bytes(&buf)?;
    Ok(msg)
}
