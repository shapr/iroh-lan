use std::{
    collections::{BTreeMap, HashSet},
    net::Ipv4Addr,
    time::Duration,
};

use ed25519_dalek::SigningKey;
use futures::StreamExt;
use iroh_blobs::store::mem::MemStore;
use iroh_docs::{AuthorId, Entry, NamespaceSecret, api::Doc, protocol::Docs, store::Query};

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
                _endpoint: endpoint.clone(),
                endpoint_id: endpoint.id(),
                _topic: topic_handle,
                rx,
                _blobs: blobs,
                my_ip: RouterIp::NoIp,
                assignments: BTreeMap::new(),
                candidates: BTreeMap::new(),
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

    pub(crate) _blobs: MemStore,
    pub(crate) _docs: Docs,
    pub(crate) doc: Doc,
    pub(crate) author_id: AuthorId,

    pub(crate) _endpoint: Endpoint,
    pub endpoint_id: EndpointId,
    pub(crate) _topic: Option<TopicDiscoveryHandle>,

    pub my_ip: RouterIp,

    assignments: BTreeMap<Ipv4Addr, IpAssignment>,
    candidates: BTreeMap<Ipv4Addr, BTreeMap<EndpointId, IpCandidate>>,
}

#[derive(Debug)]
enum DocCacheUpdate {
    UpsertAssignment(IpAssignment),
    UpsertCandidate(IpCandidate),
}

async fn handle_doc_entry(
    entry: Entry,
    updates: &tokio::sync::mpsc::UnboundedSender<DocCacheUpdate>,
) -> Result<()> {
    let key = match std::str::from_utf8(entry.key()) {
        Ok(k) => k,
        Err(e) => {
            warn!("Invalid entry key utf8: {}", e);
            return Err(e.into());
        }
    };

    let assigned_prefix = key_assigned_ip();
    let candidate_prefix = key_candidate_ip_prefix();

    // Assignment
    if key.starts_with(&assigned_prefix) {
        let ip_assignment = match decode_ip_assignment(key) {
            Ok(a) => a,
            Err(e) => {
                warn!("Failed to decode ip assignment from key {}: {}", key, e);
                return Err(e);
            }
        };
        updates.send(DocCacheUpdate::UpsertAssignment(ip_assignment))?;
        return Ok(());
    }
    // Candidate
    else if key.starts_with(&candidate_prefix) {
        let ip_candidate = match decode_ip_candidate(key) {
            Ok(c) => c,
            Err(e) => {
                warn!("Failed to decode ip candidate from key {}: {}", key, e);
                return Err(e);
            }
        };
        updates.send(DocCacheUpdate::UpsertCandidate(ip_candidate))?;
        return Ok(());
    }

    warn!(
        "[Doc] Unknown entry key prefix, skipping processing for key {}",
        key
    );
    Err(anyhow::anyhow!("Unhandled doc entry"))
}

async fn run_doc_worker(doc: Doc, updates: tokio::sync::mpsc::UnboundedSender<DocCacheUpdate>) {
    match doc.get_many(Query::all()).await {
        Ok(stream) => {
            let entries = stream.collect::<Vec<_>>().await;
            for entry in entries.into_iter().flatten() {
                if let Err(e) = handle_doc_entry(entry, &updates).await {
                    warn!(
                        "[Doc] Failed to handle doc entry during initial population: {:?}",
                        e
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
                match event {
                    Ok(e) => match e {
                        iroh_docs::engine::LiveEvent::InsertLocal { entry }
                        | iroh_docs::engine::LiveEvent::InsertRemote { entry, .. } => {
                            if let Err(e) = handle_doc_entry(entry, &updates,).await {
                                warn!("[Doc] Failed to handle doc entry: {:?}", e);
                            };
                        }
                        _ => {}
                    },
                    Err(e) => warn!("Docs subscription stream error: {:?}", e),
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
        tokio::spawn(async move {
            run_doc_worker(doc, doc_update_tx).await;
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
                        Ok(e) => debug!("Gossip monitor event: {:?}", e),
                        Err(e) => warn!("Gossip monitor stream error: {:?}", e),
                    }
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
                        DocCacheUpdate::UpsertAssignment(ip_assignment) => {
                            self.assignments.insert(ip_assignment.ip, ip_assignment);
                        }
                        DocCacheUpdate::UpsertCandidate(ip_candidate) => {
                            match self.candidates.get_mut(&ip_candidate.ip) {
                                Some(cands) => {
                                    cands.insert(ip_candidate.endpoint_id, ip_candidate);
                                }
                                None => {
                                    let mut map = BTreeMap::new();
                                    map.insert(ip_candidate.endpoint_id, ip_candidate.clone());
                                    self.candidates.insert(ip_candidate.ip, map);
                                },
                            }
                        }
                    }
                }
            }
        }
        warn!("RouterActor stopped.");
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

fn decode_ip_assignment(key: &str) -> Result<IpAssignment> {
    let parts: Vec<&str> = key.split('/').collect();
    if parts.len() != 6 {
        warn!(
            "Invalid key format for IpAssignment (invalid length): {}",
            key
        );
        anyhow::bail!("Invalid key format for IpAssignment");
    }
    if let (Ok(ip), Ok(endpoint_id), Ok(timestamp)) = (
        parts[3].parse::<Ipv4Addr>(),
        parts[4].parse::<EndpointId>(),
        parts[5].parse::<u64>(),
    ) {
        Ok(IpAssignment {
            ip,
            endpoint_id,
            last_updated: timestamp,
        })
    } else {
        warn!(
            "Failed to parse IpAssignment from key (parse failed): {}",
            key
        );
        anyhow::bail!("Invalid key format for IpAssignment");
    }
}

fn encode_ip_assignment(assignment: &IpAssignment) -> String {
    format!(
        "/assigned/ip/{}/{}/{}",
        assignment.ip, assignment.endpoint_id, assignment.last_updated
    )
}

fn key_assigned_ip() -> String {
    "/assigned/ip/".to_string()
}

fn decode_ip_candidate(key: &str) -> Result<IpCandidate> {
    let parts: Vec<&str> = key.split('/').collect();
    if parts.len() != 6 {
        warn!(
            "Invalid key format for IpCandidate (invalid length): {}",
            key
        );
        anyhow::bail!("Invalid key format for IpCandidate");
    }
    if let (Ok(ip), Ok(endpoint_id), Ok(timestamp)) = (
        parts[3].parse::<Ipv4Addr>(),
        parts[4].parse::<EndpointId>(),
        parts[5].parse::<u64>(),
    ) {
        Ok(IpCandidate {
            ip,
            endpoint_id,
            last_updated: timestamp,
        })
    } else {
        warn!(
            "Failed to parse IpCandidate from key (parse failed): {}",
            key
        );
        anyhow::bail!("Invalid key format for IpCandidate");
    }
}

fn encode_ip_candidate(candidate: &IpCandidate) -> String {
    format!(
        "/candidates/ip/{}/{}/{}",
        candidate.ip, candidate.endpoint_id, candidate.last_updated
    )
}

fn key_candidate_ip_prefix() -> String {
    "/candidates/ip/".to_string()
}

impl RouterActor {
    const ASSIGNMENT_STALE_SECS: u64 = 300;
    const CANDIDATE_STALE_SECS: u64 = 60;

    fn read_all_ip_assignments(&self) -> Result<Vec<IpAssignment>> {
        Ok(self
            .assignments
            .values()
            .filter(|assignment| !assignment.is_stale())
            .cloned()
            .collect::<Vec<_>>())
    }

    fn read_all_ip_candidates(&self) -> Result<Vec<IpCandidate>> {
        Ok(self
            .candidates
            .values()
            .flatten()
            .filter(|(_, candidate)| !candidate.is_stale())
            .map(|(_, candidate)| candidate.clone())
            .collect::<Vec<_>>())
    }

    // Assigned IPs
    fn read_ip_assignment(&self, ip: Ipv4Addr) -> Result<Option<IpAssignment>> {
        let mut assignments = self
            .assignments
            .iter()
            .filter(|(_, assignment)| !assignment.is_stale())
            .map(|(_, assignment)| assignment)
            .collect::<Vec<_>>();
        assignments.sort_by_key(|a| a.last_updated);
        Ok(assignments.into_iter().rev().find(|a| a.ip == ip).cloned())
    }

    // Candidate IPs
    fn read_ip_candidates(&self, ip: Ipv4Addr) -> Result<Vec<IpCandidate>> {
        let mut ip_candidates = match self.candidates.get(&ip) {
            Some(candidates) => candidates
                .values()
                .filter(|candidate| !candidate.is_stale())
                .cloned()
                .collect::<Vec<_>>(),
            None => vec![],
        };
        ip_candidates.sort_by_key(|candidate| candidate.last_updated);
        debug!("candidates for {ip}: {}", ip_candidates.len());
        Ok(ip_candidates)
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
        let now = current_time();
        let ip_assignment = IpAssignment {
            ip,
            endpoint_id,
            last_updated: now,
        };
        self.doc
            .set_bytes(
                self.author_id,
                encode_ip_assignment(&ip_assignment),
                now.to_le_bytes().to_vec(),
            )
            .await?;
        debug!("Wrote IP assignment {} for {}.", ip, endpoint_id);

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
        let now = current_time();
        let candidate = IpCandidate {
            ip,
            endpoint_id,
            last_updated: now,
        };
        self.doc
            .set_bytes(
                self.author_id,
                encode_ip_candidate(&candidate),
                now.to_le_bytes().to_vec(),
            )
            .await?;
        debug!("Wrote IP candidate {} for {}.", ip, endpoint_id);

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
