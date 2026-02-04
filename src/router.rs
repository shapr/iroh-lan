use std::{
    collections::{BTreeMap, HashMap, HashSet},
    net::Ipv4Addr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use ed25519_dalek::SigningKey;
use futures::StreamExt;
use iroh_blobs::{api::downloader::Downloader, store::mem::MemStore};
use iroh_docs::{AuthorId, Entry, NamespaceSecret, api::Doc, protocol::Docs, store::Query};
use tokio::{sync::watch, time::timeout};

use anyhow::{Result, bail};
use iroh::{Endpoint, EndpointAddr, EndpointId, SecretKey};
use iroh_gossip::{
    api::{GossipReceiver, GossipSender},
    net::Gossip,
};
use iroh_topic_tracker::{
    TopicDiscoveryConfig, TopicDiscoveryExt, TopicDiscoveryHandle, TopicDiscoveryHook,
};
use rand::Rng;
use serde::{Deserialize, Serialize};
use sha2::Digest;
use tracing::{debug, info, trace, warn};

use actor_helper::{Action, Actor, Handle, act, act_ok};

#[derive(Debug, Clone)]
pub struct Builder {
    topic_discovery_hook: TopicDiscoveryHook,
    entry_name: String,
    secret_key: SecretKey,
    password: String,
    endpoint: Option<Endpoint>,
    gossip: Option<Gossip>,
    docs: Option<Docs>,
    blobs: MemStore,
}

impl Builder {
    pub fn new(topic_discovery_hook: TopicDiscoveryHook) -> Builder {
        Builder {
            topic_discovery_hook,
            entry_name: String::default(),
            secret_key: SecretKey::generate(&mut rand::rng()),
            password: String::default(),
            endpoint: None,
            gossip: None,
            docs: None,
            blobs: MemStore::new(),
        }
    }

    pub fn entry_name(mut self, entry_name: &str) -> Self {
        self.entry_name = entry_name.to_string();
        self
    }

    pub fn secret_key(mut self, secret_key: SecretKey) -> Self {
        self.secret_key = secret_key;
        self
    }

    pub fn password(mut self, password: &str) -> Self {
        self.password = password.to_string();
        self
    }

    pub fn endpoint(mut self, endpoint: Endpoint) -> Self {
        self.endpoint = Some(endpoint);
        self
    }

    pub fn gossip(mut self, gossip: Gossip) -> Self {
        self.gossip = Some(gossip);
        self
    }

    pub fn docs(mut self, docs: Docs) -> Self {
        self.docs = Some(docs);
        self
    }

    pub fn blobs(mut self, blobs: MemStore) -> Self {
        self.blobs = blobs;
        self
    }

    pub async fn build(&self) -> Result<Router> {
        let endpoint = if let Some(ep) = &self.endpoint {
            ep.clone()
        } else {
            bail!("endpoint must be set");
        };
        let gossip = if let Some(g) = &self.gossip {
            g.clone()
        } else {
            bail!("gossip must be set");
        };
        let docs = if let Some(d) = &self.docs {
            d.clone()
        } else {
            bail!("docs must be set");
        };
        let blobs = self.blobs.clone();

        let topic_initials = format!("lanparty-{}", self.entry_name);
        let secret_initials = format!("{topic_initials}-{}-secret", self.password)
            .as_bytes()
            .to_vec();

        let mut topic_hasher = sha2::Sha512::new();
        topic_hasher.update("iroh-lan-topic");
        topic_hasher.update(&secret_initials);
        let topic_hash: [u8; 32] = topic_hasher.finalize()[..32].try_into()?;

        let signing_key = SigningKey::from_bytes(&self.secret_key.to_bytes());
        let topic_discovery_config =
            TopicDiscoveryConfig::new(signing_key, self.topic_discovery_hook.clone());
        let (gossip_sender, gossip_receiver, topic_handle) = loop {
            if let Ok((gossip_sender, gossip_receiver, topic_handle)) = gossip
                .subscribe_with_discovery_joined(
                    topic_hash.to_vec(),
                    vec![],
                    topic_discovery_config.clone(),
                )
                .await
            {
                break (gossip_sender, gossip_receiver, Some(topic_handle));
            } else {
                warn!("Failed to join topic; retrying in 2 second");
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        };

        let doc_peers = gossip_receiver
            .neighbors()
            .map(EndpointAddr::new)
            .collect::<Vec<_>>();
        info!("Initial doc peers: {:?}", doc_peers);
        let (doc_peers_tx, _doc_peers_rx) = watch::channel(doc_peers.clone());

        debug!("[Doc peers]: {:?}", doc_peers);

        let author_id = docs.author_create().await?;
        let doc = docs
            .import(iroh_docs::DocTicket {
                capability: iroh_docs::Capability::Write(NamespaceSecret::from_bytes(&topic_hash)),
                nodes: doc_peers.clone(),
            })
            .await?;

        while match doc.get_sync_peers().await {
            Ok(Some(peers)) => peers.is_empty(),
            Ok(None) => true,
            Err(_) => true,
        } {
            doc.start_sync(
                gossip_receiver
                    .neighbors()
                    .map(EndpointAddr::new)
                    .collect::<Vec<_>>(),
            )
            .await
            .ok();
            debug!("Waiting for doc to be ready...");
            tokio::time::sleep(Duration::from_millis(1000)).await;
        }

        let (api, rx) = Handle::channel();
        tokio::spawn(async move {
            let mut router_actor = RouterActor {
                _gossip_sender: gossip_sender,
                gossip_monitor: gossip_receiver,
                author_id,
                _docs: docs,
                doc,
                endpoint: endpoint.clone(),
                endpoint_id: endpoint.id(),
                _topic: topic_handle,
                rx,
                blobs,
                my_ip: RouterIp::NoIp,
                assignments: BTreeMap::new(),
                candidates: BTreeMap::new(),
                pending_assignments: Vec::new(),
                pending_candidates: Vec::new(),
                doc_peers_tx,
            };
            router_actor.run().await
        });

        Ok(Router { api })
    }
}

#[derive(Debug, Clone)]
pub struct Router {
    api: Handle<RouterActor, anyhow::Error>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouterIp {
    NoIp,
    AquiringIp(IpCandidate, tokio::time::Instant),
    VerifyingIp(Ipv4Addr, tokio::time::Instant),
    AssignedIp(Ipv4Addr),
}

#[derive(Debug)]
struct RouterActor {
    pub(crate) rx: actor_helper::Receiver<Action<RouterActor>>,

    pub _gossip_sender: GossipSender,
    pub gossip_monitor: GossipReceiver,

    pub(crate) blobs: MemStore,
    pub(crate) _docs: Docs,
    pub(crate) doc: Doc,
    pub(crate) author_id: AuthorId,

    pub(crate) endpoint: Endpoint,
    pub endpoint_id: EndpointId,
    pub(crate) _topic: Option<TopicDiscoveryHandle>,

    pub my_ip: RouterIp,

    assignments: BTreeMap<Ipv4Addr, (IpAssignment, Entry)>,
    candidates: BTreeMap<(Ipv4Addr, EndpointId), (IpCandidate, Entry)>,
    pending_assignments: Vec<Entry>,
    pending_candidates: Vec<Entry>,
    doc_peers_tx: watch::Sender<Vec<EndpointAddr>>,
}

#[derive(Debug)]
enum DocCacheUpdate {
    UpsertAssignment(IpAssignment, Entry),
    RemoveAssignment(Ipv4Addr, Entry),
    UpsertCandidate(IpCandidate, Entry),
    RemoveCandidate(Ipv4Addr, EndpointId, Entry),
    PendingAssignment(Entry),
    PendingCandidate(Entry),
}

async fn handle_doc_entry(
    entry: Entry,
    blobs: &MemStore,
    updates: &tokio::sync::mpsc::UnboundedSender<DocCacheUpdate>,
    pending: &mut HashMap<iroh_blobs::Hash, Vec<Entry>>,
    download_expected: bool,
    downloader: Downloader,
    peer_ids: Vec<EndpointId>,
) -> bool {
    let key = match std::str::from_utf8(entry.key()) {
        Ok(k) => k,
        Err(e) => {
            warn!("Invalid entry key utf8: {}", e);
            return false;
        }
    };

    let assigned_prefix = key_ip_assigned_prefix();
    let candidate_prefix = key_ip_candidate_prefix();

    if key.starts_with(&assigned_prefix) {
        updates
            .send(DocCacheUpdate::PendingAssignment(entry.clone()))
            .ok();
    } else if key.starts_with(&candidate_prefix) {
        updates
            .send(DocCacheUpdate::PendingCandidate(entry.clone()))
            .ok();
    }

    if entry.content_len() == 0 {
        info!(
            "[Router] Removing entry for key {} - hash={}",
            key,
            entry.content_hash()
        );
        if key.starts_with(&assigned_prefix) {
            if let Some(ip_str) = key.strip_prefix(&assigned_prefix)
                && let Ok(ip) = ip_str.parse::<Ipv4Addr>()
            {
                updates
                    .send(DocCacheUpdate::RemoveAssignment(ip, entry.clone()))
                    .ok();
            }
        } else if key.starts_with(&candidate_prefix) {
            let parts: Vec<&str> = key.split('/').collect();
            if parts.len() >= 5
                && let (Ok(ip), Ok(eid)) =
                    (parts[3].parse::<Ipv4Addr>(), parts[4].parse::<EndpointId>())
            {
                updates
                    .send(DocCacheUpdate::RemoveCandidate(ip, eid, entry.clone()))
                    .ok();
            }
        }
        warn!(
            "[Doc] Entry has zero content length, skipping blob fetch for key {}",
            key
        );
        return false;
    }

    let val_bytes = match blobs.get_bytes(entry.content_hash()).await {
        Ok(b) => {
            debug!("Blob found for entry key {}", entry.content_hash());
            b
        }
        Err(e) => {
            let hash = entry.content_hash();
            pending.entry(hash).or_default().push(entry.clone());
            let err_str = e.to_string();
            debug!("[Doc] Blob get_bytes failed for {}: {}", hash, err_str);
            if download_expected {
                warn!(
                    "[Doc] Blob not found for entry key {} ({:?}), deferring processing",
                    hash, e
                );

                let download_res = timeout(
                    Duration::from_secs(5),
                    downloader.download(hash, peer_ids.clone()),
                )
                .await;
                if matches!(download_res, Err(_) | Ok(Err(_))) {
                    warn!(
                        "[Doc] Failed to download pending blob {}: {:?}",
                        hash, download_res
                    );
                    return true;
                }
                if let Ok(b) = blobs.get_bytes(hash).await {
                    debug!("[Doc] Successfully downloaded pending blob {}", hash);
                    b
                } else {
                    warn!(
                        "[Doc] Blob still not found for entry key {} after download attempt, skipping processing",
                        hash
                    );
                    return true;
                }
            } else {
                warn!(
                    "[Doc] Blob not found for entry key {} ({:?}), skipping processing",
                    hash, e
                );
                return false;
            }
        }
    };

    // only tag after get_bytes succeeds
    blobs.tags().create(entry.content_hash()).await.ok();

    if key.starts_with(&assigned_prefix) {
        match postcard::from_bytes::<IpAssignment>(&val_bytes) {
            Ok(val) => {
                updates
                    .send(DocCacheUpdate::UpsertAssignment(val, entry.clone()))
                    .ok();
            }
            Err(e) => warn!("Failed to deserialize assignment: {}", e),
        }
    } else if key.starts_with(&candidate_prefix) {
        match postcard::from_bytes::<IpCandidate>(&val_bytes) {
            Ok(val) => {
                updates
                    .send(DocCacheUpdate::UpsertCandidate(val, entry.clone()))
                    .ok();
            }
            Err(e) => warn!("Failed to deserialize candidate: {}", e),
        }
    } else {
        warn!(
            "[Doc] Unknown entry key prefix, skipping processing for key {}",
            key
        );
    }
    false
}

fn schedule_resync_doc(
    doc: Doc,
    blobs: MemStore,
    peers_rx: watch::Receiver<Vec<EndpointAddr>>,
    resync_in_flight: Arc<AtomicBool>,
    pending_hashes: Vec<iroh_blobs::Hash>,
    downloader: Downloader,
) {
    if resync_in_flight
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        .is_err()
    {
        info!("skipping resync, already inflight");
        return;
    }
    info!(
        "resync docs: {:?}",
        resync_in_flight.load(Ordering::Relaxed)
    );

    tokio::spawn(async move {
        let peers = peers_rx.borrow().clone();
        if !peers.is_empty() {
            let res = tokio::time::timeout(Duration::from_secs(30), async {
                if let Err(e) = doc.start_sync(peers.clone()).await {
                    warn!("[Doc][resync] Failed to start doc sync for pending blobs: {}", e);
                    return Err(anyhow::anyhow!(e));
                }
                if let Err(e) = blobs.sync_db().await {
                    warn!("[Doc][resync] Failed to sync blobs db for pending blobs: {}", e);
                    return Err(anyhow::anyhow!(e));
                }

                let peers = peers.clone();
                let peer_ids: Vec<_> = peers.iter().map(|e| e.id).collect();
                for hash in pending_hashes {
                    if let Err(err) = downloader.download(hash, peer_ids.clone()).await {
                        warn!("[Doc][resync] Failed to download pending blob {}: {}", hash, err);
                        anyhow::bail!(err)
                    }
                    if let Err(err) = blobs.get_bytes(hash).await {
                        warn!(
                            "[Doc][resync] Failed to get bytes for pending blob {} after download: {}",
                            hash, err
                        );
                        anyhow::bail!(err)
                    } else {
                        debug!("[Doc][resync] Successfully downloaded pending blob {}", hash);
                    }
                }
                Ok(())
            })
            .await;
            if let Err(e) = res {
                warn!("[Doc][resync] Doc resync timed out: {}", e);
            } else if let Ok(Err(e)) = res {
                warn!(
                    "[Doc][resync] Failed to resync doc for pending blobs: {}",
                    e
                );
            }
        } else {
            warn!("[Doc][resync] No peers available for doc resync, skipping resync");
        }
        resync_in_flight.store(false, Ordering::SeqCst);
    });
}

async fn run_doc_worker(
    doc: Doc,
    blobs: MemStore,
    doc_peers_rx: watch::Receiver<Vec<EndpointAddr>>,
    updates: tokio::sync::mpsc::UnboundedSender<DocCacheUpdate>,
    endpoint: Endpoint,
) {
    let mut pending_blobs: HashMap<iroh_blobs::Hash, Vec<Entry>> = HashMap::new();
    let mut retry_tick = tokio::time::interval(Duration::from_secs(5));
    retry_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let resync_in_flight = Arc::new(AtomicBool::new(false));

    let downloader = blobs.downloader(&endpoint);
    let peers = doc_peers_rx
        .borrow()
        .iter()
        .map(|e| e.id)
        .collect::<Vec<_>>();

    match doc.get_many(Query::all()).await {
        Ok(stream) => {
            let entries = stream.collect::<Vec<_>>().await;
            for entry in entries.into_iter().flatten() {
                let needs_resync = handle_doc_entry(
                    entry,
                    &blobs,
                    &updates,
                    &mut pending_blobs,
                    true,
                    downloader.clone(),
                    peers.clone(),
                )
                .await;
                if needs_resync {
                    schedule_resync_doc(
                        doc.clone(),
                        blobs.clone(),
                        doc_peers_rx.clone(),
                        resync_in_flight.clone(),
                        pending_blobs.keys().cloned().collect::<Vec<_>>(),
                        downloader.clone(),
                    );
                }
            }
        }
        Err(e) => warn!("Failed to populate cache: {}", e),
    }

    let mut docs_sub = match doc.subscribe().await {
        Ok(sub) => sub,
        Err(e) => {
            warn!("Docs subscription failed: {}", e);
            return;
        }
    };

    loop {
        tokio::select! {
            Some(event) = docs_sub.next() => {

                let peers = doc_peers_rx
                    .borrow()
                    .iter()
                    .map(|e| e.id)
                    .collect::<Vec<_>>();
                match event {
                    Ok(e) => match e {
                        iroh_docs::engine::LiveEvent::InsertLocal { entry }
                        | iroh_docs::engine::LiveEvent::InsertRemote { entry, .. } => {
                            let needs_resync =
                                handle_doc_entry(entry, &blobs, &updates, &mut pending_blobs, false, downloader.clone(), peers.clone()).await;
                            if needs_resync {
                                schedule_resync_doc(doc.clone(), blobs.clone(), doc_peers_rx.clone(), resync_in_flight.clone(), pending_blobs.keys().cloned().collect::<Vec<_>>(), downloader.clone());
                            }
                        }
                        iroh_docs::engine::LiveEvent::ContentReady { hash } => {
                            if let Some(entries) = pending_blobs.remove(&hash) {
                                for entry in entries {
                                    let needs_resync = handle_doc_entry(
                                        entry,
                                        &blobs,
                                        &updates,
                                        &mut pending_blobs,
                                        true,
                                        downloader.clone(),
                                        peers.clone()
                                    )
                                    .await;
                                    if needs_resync {
                                        schedule_resync_doc(doc.clone(), blobs.clone(), doc_peers_rx.clone(), resync_in_flight.clone(), pending_blobs.keys().cloned().collect::<Vec<_>>(), downloader.clone());
                                    }
                                }
                            }
                        }
                        iroh_docs::engine::LiveEvent::PendingContentReady => {
                            if !pending_blobs.is_empty() {
                                //schedule_resync_doc(doc.clone(), blobs.clone(), doc_peers_rx.clone(), resync_in_flight.clone());

                                let pending = std::mem::take(&mut pending_blobs);
                                for (_hash, entries) in pending {
                                    for entry in entries {
                                        let needs_resync = handle_doc_entry(
                                            entry,
                                            &blobs,
                                            &updates,
                                            &mut pending_blobs,
                                            true,
                                            downloader.clone(),
                                            peers.clone()).await;

                                        if needs_resync {
                                            schedule_resync_doc(doc.clone(), blobs.clone(), doc_peers_rx.clone(), resync_in_flight.clone(), pending_blobs.keys().cloned().collect::<Vec<_>>(), downloader.clone());
                                        }
                                    }
                                }
                            }
                        }
                        _ => {}
                    },
                    Err(e) => warn!("Docs subscription stream error: {:?}", e),
                }
            }
            _ = retry_tick.tick() => {
                if !pending_blobs.is_empty() {
                    let peers = doc_peers_rx
                        .borrow()
                        .iter()
                        .map(|e| e.id)
                        .collect::<Vec<_>>();
                    debug!("Retry tick: {} pending blobs", pending_blobs.len());
                    //schedule_resync_doc(doc.clone(), blobs.clone(), doc_peers_rx.clone(), resync_in_flight.clone());
                    let pending = std::mem::take(&mut pending_blobs);
                    for (_hash, entries) in pending {
                        for entry in entries {
                            let needs_resync = handle_doc_entry(
                                entry,
                                &blobs,
                                &updates,
                                &mut pending_blobs,
                                true,
                                downloader.clone(),
                                peers.clone())
                            .await;
                            if needs_resync {
                                schedule_resync_doc(doc.clone(), blobs.clone(), doc_peers_rx.clone(), resync_in_flight.clone(), pending_blobs.keys().cloned().collect::<Vec<_>>(), downloader.clone());
                            }
                        }
                    }
                }
            }
            else => break,
        }
    }
}

impl Router {
    pub fn builder(topic_discovery_hook: TopicDiscoveryHook) -> Builder {
        Builder::new(topic_discovery_hook)
    }

    pub async fn get_ip_state(&self) -> Result<RouterIp> {
        self.api
            .call(act_ok!(actor => async move {
                    actor.my_ip.clone()
            }))
            .await
    }

    pub async fn get_node_id(&self) -> Result<EndpointId> {
        self.api
            .call(act_ok!(actor => async move {
                    actor.endpoint_id
            }))
            .await
    }

    pub async fn get_ip_from_endpoint_id(&self, endpoint_id: EndpointId) -> Result<Ipv4Addr> {
        self.api
            .call(act!(actor => actor.get_ip_from_endpoint_id(endpoint_id)))
            .await
    }

    pub async fn get_endpoint_id_from_ip(&self, ip: Ipv4Addr) -> Result<EndpointId> {
        self.api
            .call(act!(actor => actor.get_endpoint_id_from_ip(ip)))
            .await
    }

    pub async fn get_peers(&self) -> Result<Vec<(EndpointId, Option<Ipv4Addr>)>> {
        self.api
            .call(act!(actor => async move {
                let mut map: BTreeMap<EndpointId, Option<Ipv4Addr>> = BTreeMap::new();

                let assignments = actor.read_all_ip_assignments()?;
                for a in assignments {
                    map.insert(a.endpoint_id, Some(a.ip));
                }

                let cands = actor.read_all_ip_candidates()?;
                for c in cands {
                    map.entry(c.endpoint_id).or_insert(None);
                }

                map.remove(&actor.endpoint_id);

                Ok(map.into_iter().collect())
            }))
            .await
    }

    pub async fn close(&self) -> Result<()> {
        self.api.call(act!(actor => actor.doc.close())).await
    }
}

impl Actor<anyhow::Error> for RouterActor {
    async fn run(&mut self) -> Result<()> {
        let mut ip_tick = tokio::time::interval(Duration::from_millis(500));
        ip_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        debug!("RouterActor started. EndpointId: {}", self.endpoint_id);

        let mut last_ip_state = self.my_ip.clone();

        let (doc_update_tx, mut doc_update_rx) = tokio::sync::mpsc::unbounded_channel();
        let doc = self.doc.clone();
        let blobs = self.blobs.clone();
        let doc_peers_rx = self.doc_peers_tx.subscribe();
        let endpoint = self.endpoint.clone();
        tokio::spawn(async move {
            run_doc_worker(doc, blobs, doc_peers_rx, doc_update_tx, endpoint).await;
        });
        loop {
            tokio::select! {
                Ok(action) = self.rx.recv_async() => {
                    trace!("Action received");
                    action(self).await;
                }
                _ = tokio::signal::ctrl_c() => {
                    info!("Received Ctrl-C, stopping RouterActor");
                    break
                }

                Some(event) = self.gossip_monitor.next() => {
                    match event {
                        Ok(e) => {
                            if rand::rng().random_bool(0.1) {
                                warn!("Gossip monitor: Unhandled event (potential backpressure source): {:?}", e);
                            }
                        }
                        Err(e) => warn!("Gossip monitor stream error: {:?}", e),
                    }
                    let peers = self
                        .gossip_monitor
                        .neighbors()
                        .map(EndpointAddr::new)
                        .collect::<Vec<_>>();
                    let _ = self.doc_peers_tx.send(peers);
                }

                // advance ip state machine
                _ = ip_tick.tick() => {
                    trace!("IP tick");
                    match self.advance_ip_state().await {
                        Ok(_) => if self.my_ip != last_ip_state {
                            info!("Router IP state changed: {:?} -> {:?}", last_ip_state, self.my_ip);
                            last_ip_state = self.my_ip.clone();
                        },
                        Err(e) => {
                            warn!("Error advancing IP state: {}", e);
                        }
                    }
                }

                Some(update) = doc_update_rx.recv() => {
                    match update {
                        DocCacheUpdate::UpsertAssignment(val, entry) => {
                            self.pending_assignments.retain(|h| h.content_hash() != entry.content_hash() && !is_stale(h.record().timestamp(), RouterActor::ASSIGNMENT_STALE_SECS));
                            self.assignments.insert(val.ip, (val, entry));
                        }
                        DocCacheUpdate::RemoveAssignment(ip, entry) => {
                            self.pending_assignments.retain(|h| h.content_hash() != entry.content_hash() && !is_stale(h.record().timestamp(), RouterActor::ASSIGNMENT_STALE_SECS));
                            self.assignments.remove(&ip);
                        }
                        DocCacheUpdate::UpsertCandidate(val, entry) => {
                            self.pending_candidates.retain(|h| h.content_hash() != entry.content_hash() && !is_stale(h.record().timestamp(), RouterActor::CANDIDATE_STALE_SECS));
                            self.candidates.insert((val.ip, val.endpoint_id), (val, entry));
                        }
                        DocCacheUpdate::RemoveCandidate(ip, endpoint_id, entry) => {
                            self.pending_candidates.retain(|h| h.content_hash() != entry.content_hash() && !is_stale(h.record().timestamp(), RouterActor::CANDIDATE_STALE_SECS));
                            self.candidates.remove(&(ip, endpoint_id));
                        }
                        DocCacheUpdate::PendingAssignment(hash) => {
                            self.pending_assignments.push(hash);
                        }
                        DocCacheUpdate::PendingCandidate(hash) => {
                            self.pending_candidates.push(hash);
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

fn current_time() -> u64 {
    chrono::Utc::now().timestamp() as u64
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IpAssignment {
    pub ip: Ipv4Addr,
    pub endpoint_id: EndpointId,
    pub last_updated: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct IpCandidate {
    pub ip: Ipv4Addr,
    pub endpoint_id: EndpointId,
    pub last_updated: u64,
}

impl IpAssignment {
    pub fn is_stale(&self) -> bool {
        is_stale(self.last_updated, RouterActor::ASSIGNMENT_STALE_SECS)
    }
}

impl IpCandidate {
    pub fn is_stale(&self) -> bool {
        is_stale(self.last_updated, RouterActor::CANDIDATE_STALE_SECS)
    }
}

fn is_stale(last_updated: u64, stale_secs: u64) -> bool {
    current_time().saturating_sub(last_updated) > stale_secs
}

fn key_ip_assigned(ip: Ipv4Addr) -> String {
    format!("/assigned/ip/{ip}")
}

fn key_ip_assigned_prefix() -> String {
    "/assigned/ip/".to_string()
}

fn key_ip_candidate(ip: Ipv4Addr, endpoint_id: EndpointId) -> String {
    format!("/candidates/ip/{ip}/{endpoint_id}")
}

fn key_ip_candidate_prefix() -> String {
    "/candidates/ip/".to_string()
}

impl RouterActor {
    const ASSIGNMENT_STALE_SECS: u64 = 300;
    const CANDIDATE_STALE_SECS: u64 = 60;

    fn read_all_ip_assignments(&self) -> Result<Vec<IpAssignment>> {
        if self
            .pending_assignments
            .iter()
            .filter(|entry| {
                !is_stale(
                    entry.record().timestamp(),
                    RouterActor::ASSIGNMENT_STALE_SECS,
                )
            })
            .count()
            > 0
        {
            warn!("[Doc] read_all_ip_assignments: there are pending blob assignments",);
            anyhow::bail!("ip is already assigned (pending)");
        }
        Ok(self
            .assignments
            .values()
            .filter(|(assignment, _)| !assignment.is_stale())
            .map(|(assignment, _)| assignment.clone())
            .collect::<Vec<_>>())
    }

    fn read_all_ip_candidates(&self) -> Result<Vec<IpCandidate>> {
        if self
            .pending_assignments
            .iter()
            .filter(|entry| {
                !is_stale(
                    entry.record().timestamp(),
                    RouterActor::CANDIDATE_STALE_SECS,
                )
            })
            .count()
            > 0
        {
            warn!("[Doc] read_all_ip_candidates: there are pending blob assignments",);
            anyhow::bail!("ip is already assigned (pending)");
        }
        Ok(self
            .candidates
            .values()
            .filter(|(candidate, _)| !candidate.is_stale())
            .map(|(candidate, _)| candidate.clone())
            .collect::<Vec<_>>())
    }

    // Assigned IPs
    fn read_ip_assignment(&self, ip: Ipv4Addr) -> Result<Option<IpAssignment>> {
        if self
            .pending_assignments
            .iter()
            .filter(|entry| {
                !is_stale(
                    entry.record().timestamp(),
                    RouterActor::ASSIGNMENT_STALE_SECS,
                )
            })
            .any(|entry| {
                if let Some(key_str) = std::str::from_utf8(entry.key()).ok()
                    && key_str == key_ip_assigned(ip)
                {
                    return true;
                }
                false
            })
        {
            warn!(
                "[Doc] read_ip_assignment: ip {} has pending blob assignments",
                ip
            );
            anyhow::bail!("ip is already assigned (pending)");
        }
        Ok(self.assignments
            .iter()
            .filter(|(_, (assignment, _))| !assignment.is_stale())
            .find(|(key, _)| *key == &ip)
            .map(|(_, (value, _))| value.clone()))
    }

    // Candidate IPs
    fn read_ip_candidates(&self, ip: Ipv4Addr) -> Result<Vec<IpCandidate>> {
        if self
            .pending_assignments
            .iter()
            .filter(|entry| {
                !is_stale(
                    entry.record().timestamp(),
                    RouterActor::CANDIDATE_STALE_SECS,
                )
            })
            .any(|entry| {
                if let Some(key_str) = std::str::from_utf8(entry.key()).ok()
                    && key_str == key_ip_assigned(ip)
                {
                    return true;
                }
                false
            })
        {
            warn!(
                "[Doc] read_ip_candidates: ip {} has pending blob assignments",
                ip
            );
            anyhow::bail!("ip is already assigned (pending)");
        }
        let filtered = self
            .candidates
            .values()
            .filter(|(c, _)| c.ip == ip)
            .filter(|(candidate, _)| !candidate.is_stale())
            .map(|(candidate, _)| candidate.clone())
            .collect::<Vec<_>>();
        debug!("candidates for {ip}: {}", filtered.len());
        Ok(filtered)
    }

    // write ip assigned
    async fn write_ip_assignment(&mut self, ip: Ipv4Addr, endpoint_id: EndpointId) -> Result<()> {
        info!("Attempting to assign IP {} to {}", ip, endpoint_id);

        let existing_assignment = self.read_ip_assignment(ip)?;

        if let Some(existing) = &existing_assignment {
            if existing.endpoint_id != endpoint_id {
                anyhow::bail!("ip already assigned to another node");
            }
        } else {
            // New assignment. check for multiple candidates
            let candidates = self.read_ip_candidates(ip)?;
            if candidates.is_empty() {
                bail!("no candidates for this ip");
            }

            if candidates
                .iter()
                .any(|c| c.endpoint_id != self.endpoint_id && self.endpoint_id < c.endpoint_id)
            {
                bail!("another candidate with higher endpoint_id exists");
            }
        }

        // We are the chosen one!
        debug!("Winning candidate for IP {}. Writing assignment.", ip);
        let data = postcard::to_stdvec(&IpAssignment {
            ip,
            endpoint_id,
            last_updated: current_time(),
        })?;
        let size = data.len() as u64;

        let tag = match self.blobs.add_bytes(data).await {
            Ok(tag) => tag,
            Err(err) => {
                warn!(
                    "[Router] write_ip_assignment: blob add_bytes failed: {:?}",
                    err
                );
                bail!(err)
            }
        };

        if let Err(e) = self.blobs.get_bytes(tag.hash).await {
            warn!(
                "[Router] write_ip_assignment: blob get_bytes failed after add: {:?}",
                e
            );
            bail!(e)
        }

        if let Err(e) = self
            .doc
            .set_hash(self.author_id, key_ip_assigned(ip), tag.hash, size)
            .await
        {
            warn!("[Router] write_ip_assignment: doc set_hash failed: {:?}", e);
            bail!(e)
        } else {
            debug!(
                "[Router] Wrote IP assignment {} for {} - hash={}",
                ip, endpoint_id, tag.hash
            );
        }

        self.blobs.sync_db().await.ok();

        Ok(())
    }

    // write ip candidate
    async fn write_ip_candidate(
        &mut self,
        ip: Ipv4Addr,
        endpoint_id: EndpointId,
    ) -> Result<IpCandidate> {
        // already assigned? don't write
        if self.read_ip_assignment(ip)?.is_some() {
            anyhow::bail!("ip assignment already assigned");
        }

        if let Ok(candidates) = self.read_ip_candidates(ip)
            && !candidates.is_empty()
        {
            anyhow::bail!("ip candidate already exists");
        }

        // idempotent for our own node_id
        let candidate = IpCandidate {
            ip,
            endpoint_id,
            last_updated: current_time(),
        };
        let data = postcard::to_stdvec(&candidate)?;
        let size = data.len() as u64;

        let tag = match self.blobs.add_bytes(data).await {
            Ok(tag) => tag,
            Err(err) => {
                warn!(
                    "[Router] write_ip_candidate: blob add_bytes failed: {:?}",
                    err
                );
                bail!(err)
            }
        };
        if let Err(e) = self.blobs.get_bytes(tag.hash).await {
            warn!(
                "[Router] write_ip_candidate: blob get_bytes failed after add: {:?}",
                e
            );
            bail!(e)
        }

        if let Err(e) = self
            .doc
            .set_hash(
                self.author_id,
                key_ip_candidate(ip, endpoint_id),
                tag.hash,
                size,
            )
            .await
        {
            warn!("[Router] write_ip_candidate: doc set_hash failed: {:?}", e);
            bail!(e)
        } else {
            debug!(
                "[Router] Wrote IP candidate {} for {} - hash={}",
                ip, endpoint_id, tag.hash
            );
        }

        self.blobs.sync_db().await.ok();

        Ok(candidate)
    }

    async fn get_endpoint_id_from_ip(&mut self, ip: Ipv4Addr) -> Result<EndpointId> {
        self.read_ip_assignment(ip)?
            .map(|a| a.endpoint_id)
            .ok_or_else(|| anyhow::anyhow!("no endpoint_id found for ip"))
    }

    async fn get_ip_from_endpoint_id(&mut self, endpoint_id: EndpointId) -> Result<Ipv4Addr> {
        self.read_all_ip_assignments()?
            .iter()
            .find(|assignment| assignment.endpoint_id == endpoint_id)
            .map(|a| a.ip)
            .ok_or_else(|| anyhow::anyhow!("no ip found for endpoint_id"))
    }
}

impl RouterActor {
    async fn advance_ip_state(&mut self) -> Result<bool> {
        trace!("Advancing IP state. Current: {:?}", self.my_ip);
        match self.my_ip.clone() {
            RouterIp::NoIp => {
                let next_ip = self.get_next_ip().await?;
                info!("Trying to acquire IP candidate: {}", next_ip);

                self.my_ip = RouterIp::AquiringIp(
                    self.write_ip_candidate(next_ip, self.endpoint_id).await?,
                    tokio::time::Instant::now(),
                );
                Ok(false)
            }
            RouterIp::AquiringIp(ip_candidate, start_time) => {
                let elapsed = start_time.elapsed();

                if let Ok(candidates) = self.read_ip_candidates(ip_candidate.ip)
                    && candidates.iter().any(|c| {
                        c.endpoint_id != self.endpoint_id && self.endpoint_id < c.endpoint_id
                    })
                {
                    warn!(
                        "Conflict detected for {:?} during acquisition wait. Aborting.",
                        ip_candidate.ip
                    );
                    self.my_ip = RouterIp::NoIp;
                    return Ok(false);
                }

                // Wait at least 5 seconds
                if elapsed > Duration::from_secs(5) {
                    // Jitter: 20% chance to proceed per tick (approx 500ms)
                    if rand::rng().random_bool(0.2) {
                        info!(
                            "Attempting to finalize IP assignment for {}",
                            ip_candidate.ip
                        );
                        if self
                            .write_ip_assignment(ip_candidate.ip, ip_candidate.endpoint_id)
                            .await
                            .is_ok()
                        {
                            info!(
                                "Written assignment for IP: {}. Verifying...",
                                ip_candidate.ip
                            );
                            self.my_ip =
                                RouterIp::VerifyingIp(ip_candidate.ip, tokio::time::Instant::now());
                        } else {
                            warn!(
                                "Failed to write assignment for {}. Restarting negotiation.",
                                ip_candidate.ip
                            );
                            self.my_ip = RouterIp::NoIp;
                        }
                    }
                }
                Ok(false)
            }
            RouterIp::VerifyingIp(ip, start_time) => {
                // Wait 2 seconds to ensure propagation
                if start_time.elapsed() > Duration::from_secs(2) {
                    trace!("Verifying IP assignment logic for {}", ip);
                    let assignment = self.read_ip_assignment(ip)?;
                    match assignment {
                        Some(a) if a.endpoint_id == self.endpoint_id => {
                            info!("Verified ownership of IP: {}", ip);
                            self.my_ip = RouterIp::AssignedIp(ip);
                        }
                        _ => {
                            warn!(
                                "Verification failed for IP: {}. Lost to another node or ghost.",
                                ip
                            );
                            self.my_ip = RouterIp::NoIp;
                        }
                    }
                }
                Ok(false)
            }
            RouterIp::AssignedIp(my_ip) => {
                match self.read_ip_assignment(my_ip)? {
                    Some(ip_assignment) => {
                        if ip_assignment.endpoint_id != self.endpoint_id {
                            self.my_ip = RouterIp::NoIp;
                            warn!(
                                "Lost IP assignment for {}. Expected owner: {}, Found: {}. Restarting negotiation.",
                                my_ip, self.endpoint_id, ip_assignment.endpoint_id
                            );
                            Ok(false)
                        } else {
                            // Refresh if needed (every 30s)
                            if current_time() - ip_assignment.last_updated > 30 {
                                info!("Refreshing IP assignment for {}", my_ip);
                                if let Err(e) =
                                    self.write_ip_assignment(my_ip, self.endpoint_id).await
                                {
                                    warn!("Failed to refresh IP assignment: {}", e);
                                    // Don't lose IP immediately, retry next tick
                                }
                            }
                            Ok(true)
                        }
                    }
                    None => {
                        self.my_ip = RouterIp::NoIp;
                        warn!("Assignment lost/deleted for {}. Restarting.", my_ip);
                        Ok(false)
                    }
                }
            }
        }
    }

    async fn get_next_ip(&mut self) -> Result<Ipv4Addr> {
        let all_assigned = self.read_all_ip_assignments()?;
        let all_candidates = self.read_all_ip_candidates()?;

        let mut used_ips = all_assigned
            .iter()
            .map(|ip_assignment| ip_assignment.ip)
            .collect::<HashSet<_>>();
        used_ips.extend(
            all_candidates
                .iter()
                .map(|ip_candidate| ip_candidate.ip)
                .collect::<HashSet<_>>(),
        );

        // Scan for the first available IP starting from 172.30.0.2
        for i in 2u16..65534 {
            let octet3 = (i >> 8) as u8;
            let octet4 = (i & 0xFF) as u8;

            // Skip network/broadcast addresses if needed, though 172.30.0.0/16 is a large subnet.
            // Keeping it simple: avoid .0 and .255 in the last octet to be safe for standard /24 clients
            if octet4 == 0 || octet4 == 255 {
                continue;
            }

            let ip = Ipv4Addr::new(172, 30, octet3, octet4);
            if !used_ips.contains(&ip) {
                return Ok(ip);
            }
        }

        bail!("no more ips available");
    }
}
