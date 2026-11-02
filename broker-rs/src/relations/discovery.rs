//! 发现、完整性与事件附着（接口第 9 节）。
//!
//! 合并四处来源，按 threadId 去重：实时关系、在途派生、持久后代（`thread/list{ancestorThreadId}`
//! 未归档与已归档各分页一遍，不加 cwd 过滤）、已加载线程（`thread/loaded/list` 加定向 `thread/read`）。
//! 完整的条件：各来源都遍历完；期间关系版本没变（变了重查一次，再变判不完整）；没有未落定的派生；
//! 已加载成员都有当前 epoch 的有效附着证据。否则 Incomplete，保留已发现成员。
//! 仅恢复本连接主动释放过的线程；同步登记发现，挂回屏障通过后再发送来源查询。
use super::*;
use serde_json::json;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SourceState {
    pub cursor: Option<String>,
    pub seen: BTreeSet<String>,
    pub pages: usize,
    pub done: bool,
}

/// 一次发现的进度；完成、到期或关闭时删除。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DiscoveryWork {
    pub candidate_versions: BTreeMap<String, u64>,
    pub check_goal_versions: BTreeMap<String, u64>,
    pub checks_queue: VecDeque<(String, String)>,
    pub checks_in_flight: BTreeSet<(String, String)>,
    pub checks_done: BTreeSet<(String, String)>,
    pub missing: BTreeMap<String, Vec<String>>,
    pub start_version: u64,
    pub rerun: bool,
    pub found: BTreeMap<String, RelationEdge>,
    pub loaded: BTreeSet<String>,
    pub reads_queue: VecDeque<String>,
    pub reads_in_flight: BTreeSet<String>,
    pub reads_total: usize,
    pub read_edges: BTreeMap<String, RelationEdge>,
    pub sources: BTreeMap<&'static str, SourceState>,
    pub reasons: BTreeSet<String>,
}

const UNARCHIVED: &str = "unarchived";
const ARCHIVED: &str = "archived";
const LOADED: &str = "loaded";

impl Machine {
    pub(super) fn discovery_begin(
        &mut self,
        root: &str,
        scope: DiscoveryScope,
        caller: DiscoveryCaller,
        actions: &mut Vec<Action>,
    ) -> u64 {
        if let DiscoveryScope::Run { run_id } = &scope {
            self.ensure_context(run_id);
        }
        let op_id = self.relations.aux.next();
        let generation = self.relations.aux.next();
        let now = self.clock.mono_ms;
        let timeout = match &caller {
            DiscoveryCaller::Converge { token, .. } => self
                .convergences
                .runs
                .get(&token.run_id)
                .map(|r| r.deadline_mono_ms.saturating_sub(now))
                .unwrap_or(0)
                .min(
                    self.runs
                        .runs
                        .get(&token.run_id)
                        .and_then(|r| r.convergence_config.as_ref())
                        .map_or(DISCOVERY_MS, |c| c.discovery_timeout_ms),
                ),
            _ => DISCOVERY_MS,
        };
        self.relations.discoveries.insert(
            op_id,
            DiscoveryOperation {
                op_id,
                generation,
                root: root.into(),
                scope,
                caller,
                started_at_ms: now,
                deadline_ms: now.saturating_add(timeout),
                completed: false,
            },
        );
        self.relations
            .aux
            .work
            .insert(op_id, DiscoveryWork::default());
        actions.push(Action::Schedule {
            token: Self::relation_token(),
            kind: TimerKind::DiscoveryDeadline { op_id, generation },
            delay_ms: timeout,
        });
        self.begin_child_barrier(root, ChildWaiter::Discovery(op_id), actions);
        if !self
            .relations
            .aux
            .child_barriers
            .contains_key(&ChildWaiter::Discovery(op_id))
            && self.relations.aux.work[&op_id].sources.is_empty()
        {
            self.discovery_start_sources(op_id, actions);
        }
        op_id
    }

    pub(super) fn discovery_start_sources(&mut self, op_id: u64, actions: &mut Vec<Action>) {
        if !self.relations.aux.work.contains_key(&op_id) {
            return;
        }
        let version = self.relations.aux.version;
        let work = self
            .relations
            .aux
            .work
            .get_mut(&op_id)
            .expect("发现进度存在");
        work.start_version = version;
        work.sources.clear();
        for source in [UNARCHIVED, ARCHIVED, LOADED] {
            work.sources.insert(source, SourceState::default());
        }
        for source in [UNARCHIVED, ARCHIVED, LOADED] {
            self.discovery_page(op_id, source, actions);
        }
    }

    fn discovery_page(&mut self, op_id: u64, source: &'static str, actions: &mut Vec<Action>) {
        let Some(op) = self.relations.discoveries.get(&op_id) else {
            return;
        };
        let generation = op.generation;
        let root = op.root.clone();
        let cursor = self.relations.aux.work[&op_id].sources[source]
            .cursor
            .clone();
        let (method, mut params) = if source == LOADED {
            ("thread/loaded/list", json!({"limit": PAGE_LIMIT}))
        } else {
            (
                "thread/list",
                json!({
                    "ancestorThreadId": root,
                    "archived": source == ARCHIVED,
                    "limit": PAGE_LIMIT,
                    "modelProviders": [],
                    "sourceKinds": ["subAgentThreadSpawn", "subAgent", "subAgentOther"],
                }),
            )
        };
        if let Some(cursor) = cursor {
            params["cursor"] = json!(cursor);
        }
        actions.push(Action::Request(RequestContext {
            resume_retried: false,
            stream_id: None,
            review_id: None,
            client: None,
            token: None,
            operation: Operation::Discovery { op_id, generation },
            method: method.into(),
            params: Some(params),
        }));
    }

    fn discovery_read(&mut self, op_id: u64, actions: &mut Vec<Action>) {
        let Some(generation) = self.relations.discoveries.get(&op_id).map(|o| o.generation) else {
            return;
        };
        let work = self
            .relations
            .aux
            .work
            .get_mut(&op_id)
            .expect("发现进度存在");
        while work.reads_in_flight.len() < DISCOVERY_READS {
            let Some(thread) = work.reads_queue.pop_front() else {
                break;
            };
            work.reads_in_flight.insert(thread.clone());
            actions.push(Action::Request(RequestContext {
                resume_retried: false,
                stream_id: None,
                review_id: None,
                client: None,
                token: None,
                operation: Operation::Discovery { op_id, generation },
                method: "thread/read".into(),
                params: Some(json!({"threadId": thread, "includeTurns": false})),
            }));
        }
    }

    pub(super) fn discovery_reply(
        &mut self,
        op_id: u64,
        generation: u64,
        context: &RequestContext,
        result: Result<Value, Value>,
        actions: &mut Vec<Action>,
    ) {
        let current = self
            .relations
            .discoveries
            .get(&op_id)
            .is_some_and(|o| !o.completed && o.generation == generation)
            && self.relations.aux.work.contains_key(&op_id);
        if !current {
            actions.push(Action::Debug("丢弃过期发现操作的回包".into()));
            return;
        }
        if let DiscoveryCaller::Converge { token, .. } = &self.relations.discoveries[&op_id].caller
        {
            if !self.convergence_token_current(token) {
                actions.push(Action::Debug("丢弃旧连接或旧管理代际的发现回包".into()));
                return;
            }
        }
        let check_thread = context
            .params
            .as_ref()
            .and_then(|p| p.get("threadId"))
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        if self.relations.aux.work[&op_id]
            .checks_in_flight
            .contains(&(check_thread.clone(), context.method.clone()))
        {
            self.convergence_discovery_check_reply(
                op_id,
                &check_thread,
                &context.method,
                result,
                actions,
            );
            self.discovery_maybe_finish(op_id, actions);
            return;
        }
        let root = self.relations.discoveries[&op_id].root.clone();
        let params = context.params.clone().unwrap_or(Value::Null);
        match context.method.as_str() {
            "thread/list" => {
                let source = if params["archived"] == true {
                    ARCHIVED
                } else {
                    UNARCHIVED
                };
                let edges: Vec<RelationEdge> = match &result {
                    Ok(value) => value
                        .get("data")
                        .and_then(Value::as_array)
                        .into_iter()
                        .flatten()
                        .filter(|t| t.get("id").and_then(Value::as_str) != Some(root.as_str()))
                        .filter_map(|t| facts::edge_from_thread(t, DiscoveredBy::AncestorList))
                        .collect(),
                    Err(_) => Vec::new(),
                };
                let work = self
                    .relations
                    .aux
                    .work
                    .get_mut(&op_id)
                    .expect("发现进度存在");
                for edge in edges {
                    work.found.insert(edge.child_thread_id.clone(), edge);
                }
                self.discovery_next(op_id, source, &result, actions);
            }
            "thread/loaded/list" => {
                let ids: Vec<String> = match &result {
                    Ok(value) => value
                        .get("data")
                        .and_then(Value::as_array)
                        .into_iter()
                        .flatten()
                        .filter_map(Value::as_str)
                        .filter(|t| *t != root)
                        .map(str::to_owned)
                        .collect(),
                    Err(_) => Vec::new(),
                };
                for id in ids {
                    let known = self.thread_relation(&id).is_some()
                        || self.relations.aux.lineage_roots.contains(&id);
                    let work = self
                        .relations
                        .aux
                        .work
                        .get_mut(&op_id)
                        .expect("发现进度存在");
                    work.loaded.insert(id.clone());
                    if known || work.found.contains_key(&id) || work.read_edges.contains_key(&id) {
                        continue;
                    }
                    if work.reads_total >= MAX_MEMBERS {
                        work.reasons.insert("tooManyLoaded".into());
                        continue;
                    }
                    if !work.reads_queue.contains(&id) && !work.reads_in_flight.contains(&id) {
                        work.reads_total += 1;
                        work.reads_queue.push_back(id);
                    }
                }
                self.discovery_next(op_id, LOADED, &result, actions);
            }
            "thread/read" => {
                let thread = params["threadId"].as_str().unwrap_or_default().to_owned();
                let work = self
                    .relations
                    .aux
                    .work
                    .get_mut(&op_id)
                    .expect("发现进度存在");
                work.reads_in_flight.remove(&thread);
                match &result {
                    Ok(value) => {
                        let info = value.get("thread").cloned().unwrap_or(Value::Null);
                        if info.get("id").and_then(Value::as_str) == Some(thread.as_str()) {
                            if let Some(edge) =
                                facts::edge_from_thread(&info, DiscoveredBy::LoadedList)
                            {
                                work.read_edges.insert(thread, edge);
                            }
                        } else {
                            work.reasons.insert("readMismatch".into());
                            work.missing
                                .entry(thread.clone())
                                .or_default()
                                .push("readMismatch".into());
                        }
                    }
                    Err(_) => {
                        work.reasons.insert("readError".into());
                        work.missing
                            .entry(thread.clone())
                            .or_default()
                            .push("readError".into());
                    }
                }
            }
            _ => {}
        }
        self.discovery_read(op_id, actions);
        self.discovery_maybe_finish(op_id, actions);
    }

    /// 分页推进：失败、重复游标、页数超限都判不完整并结束该来源。
    fn discovery_next(
        &mut self,
        op_id: u64,
        source: &'static str,
        result: &Result<Value, Value>,
        actions: &mut Vec<Action>,
    ) {
        let work = self
            .relations
            .aux
            .work
            .get_mut(&op_id)
            .expect("发现进度存在");
        let state = work.sources.get_mut(source).expect("来源存在");
        state.pages += 1;
        let next = match result {
            Err(_) => {
                work.reasons.insert(format!("pageError:{source}"));
                None
            }
            Ok(value) => value
                .get("nextCursor")
                .and_then(Value::as_str)
                .filter(|c| !c.is_empty())
                .map(str::to_owned),
        };
        let state = work.sources.get_mut(source).expect("来源存在");
        match next {
            None => state.done = true,
            Some(cursor) if state.seen.contains(&cursor) => {
                state.done = true;
                work.reasons.insert(format!("duplicateCursor:{source}"));
            }
            Some(_) if state.pages >= MAX_PAGES => {
                state.done = true;
                work.reasons.insert(format!("tooManyPages:{source}"));
            }
            Some(cursor) => {
                state.seen.insert(cursor.clone());
                state.cursor = Some(cursor);
                self.discovery_page(op_id, source, actions);
            }
        }
    }

    fn discovery_maybe_finish(&mut self, op_id: u64, actions: &mut Vec<Action>) {
        if self
            .relations
            .aux
            .child_barriers
            .contains_key(&ChildWaiter::Discovery(op_id))
        {
            return;
        }
        let Some(work) = self.relations.aux.work.get(&op_id) else {
            return;
        };
        if !work.sources.values().all(|s| s.done)
            || !work.reads_in_flight.is_empty()
            || !work.reads_queue.is_empty()
        {
            return;
        }
        if self.convergence_discovery_checks(op_id, actions) {
            return;
        }
        let work = &self.relations.aux.work[&op_id];
        // 期间关系版本变化：重查一次；再变则判不完整。
        if self.relations.aux.version != work.start_version {
            if !work.rerun {
                let generation = self.relations.aux.next();
                let op = self
                    .relations
                    .discoveries
                    .get_mut(&op_id)
                    .expect("发现操作存在");
                op.generation = generation;
                if let DiscoveryCaller::Converge { token, .. } = &op.caller {
                    if let Some(progress) = self.convergences.progress.get_mut(&token.run_id) {
                        progress.discovery_generation = generation;
                    }
                }
                let work = self
                    .relations
                    .aux
                    .work
                    .get_mut(&op_id)
                    .expect("发现进度存在");
                work.rerun = true;
                work.reads_total = 0;
                // 重查保留已发现成员，只重新遍历来源；期限不重置。
                let remaining = op_deadline_left(self, op_id);
                actions.push(Action::Schedule {
                    token: Self::relation_token(),
                    kind: TimerKind::DiscoveryDeadline { op_id, generation },
                    delay_ms: remaining,
                });
                self.discovery_start_sources(op_id, actions);
                return;
            }
            self.relations
                .aux
                .work
                .get_mut(&op_id)
                .expect("发现进度存在")
                .reasons
                .insert("versionChanged".into());
        }
        self.discovery_finish(op_id, actions);
    }

    pub(super) fn discovery_deadline(
        &mut self,
        op_id: u64,
        generation: u64,
        actions: &mut Vec<Action>,
    ) {
        if !self
            .relations
            .discoveries
            .get(&op_id)
            .is_some_and(|o| !o.completed && o.generation == generation)
        {
            return;
        }
        if let Some(work) = self.relations.aux.work.get_mut(&op_id) {
            work.reasons.insert("timeout".into());
            for thread in work.reads_queue.iter().chain(work.reads_in_flight.iter()) {
                work.missing
                    .entry(thread.clone())
                    .or_default()
                    .push("timeout".into());
            }
        }
        self.discovery_finish(op_id, actions);
    }

    /// 汇总成员：沿父边走到根（深度 ≤ 8）的才算；节点超过 256 个保留已知成员并判不完整。
    fn discovery_finish(&mut self, op_id: u64, actions: &mut Vec<Action>) {
        let Some(mut work) = self.relations.aux.work.remove(&op_id) else {
            return;
        };
        let Some(op) = self.relations.discoveries.remove(&op_id) else {
            return;
        };
        let (root, scope, caller, generation) = (
            op.root.clone(),
            op.scope.clone(),
            op.caller.clone(),
            op.generation,
        );
        actions.push(Action::Unschedule {
            key: crate::claim::TimerKey::DiscoveryDeadline(op_id),
        });
        // 某候选核实完成后，其他回包仍可在途；发布前再检查其版本。
        for (thread, version) in &work.candidate_versions {
            if thread != &root
                && self
                    .relations
                    .aux
                    .candidate_versions
                    .get(thread)
                    .copied()
                    .unwrap_or(0)
                    != *version
            {
                work.reasons.insert("candidateChanged".into());
                work.missing
                    .entry(thread.clone())
                    .or_default()
                    .push("candidateChanged".into());
            }
        }
        let mut all: BTreeMap<String, RelationEdge> = self.relations.edges.clone();
        for (thread, edge) in work.read_edges.iter().chain(work.found.iter()) {
            all.insert(thread.clone(), edge.clone());
        }
        let mut members = BTreeMap::new();
        let mut counted = 0usize;
        for (thread, edge) in &all {
            // 根深度 0：直接子线程深度 1，最深 8；多走几层只为识别「通到根但超深」。
            let mut current = edge.parent_thread_id.clone();
            let mut depth = 1;
            let mut reached = current == root;
            while !reached && depth <= 2 * MAX_DEPTH {
                match all.get(&current) {
                    Some(parent) => {
                        current = parent.parent_thread_id.clone();
                        depth += 1;
                        reached = current == root;
                    }
                    None => break,
                }
            }
            if !reached {
                continue;
            }
            if depth > MAX_DEPTH {
                work.reasons.insert("depthExceeded".into());
                continue;
            }
            if edge.kind == RelationKind::Fork {
                continue;
            }
            if edge.kind != RelationKind::ReviewExecution {
                if counted >= MAX_MEMBERS {
                    work.reasons.insert("tooManyMembers".into());
                    continue;
                }
                counted += 1;
            }
            members.insert(thread.clone(), edge.clone());
        }
        let unsettled = self.relations.contexts.values().any(|c| {
            (c.identity.logical_root_thread_id == root || c.identity.execution_thread_id == root)
                && !c.in_flight_spawns.is_empty()
        });
        if unsettled {
            work.reasons.insert("unsettledSpawns".into());
        }
        let mut attachments = BTreeMap::new();
        let mut unattached = BTreeSet::new();
        for thread in members.keys() {
            if let DiscoveryCaller::Converge { token, .. } = &caller {
                if self.classify_candidate(&token.run_id, thread) == Membership::Historical {
                    continue;
                }
            }
            let evidence = self
                .relations
                .attachments
                .get(thread)
                .filter(|a| a.valid && a.conn_epoch == self.relations.conn_epoch);
            match evidence {
                Some(a) => {
                    attachments.insert(thread.clone(), a.clone());
                }
                None if work.loaded.contains(thread) => {
                    unattached.insert(thread.clone());
                }
                None => {}
            }
        }
        if !unattached.is_empty() {
            work.reasons.insert("attachmentMissing".into());
        }
        let mut missing = work.missing;
        for thread in &unattached {
            missing
                .entry(thread.clone())
                .or_default()
                .push("attachmentMissing".into());
        }
        self.relations.aux.unobserved_loaded = unattached;
        let reasons: Vec<String> = work.reasons.into_iter().collect();
        let result = DiscoveryResult {
            root,
            scope,
            generation,
            members,
            state: DiscoveryState {
                missing,
                complete: reasons.is_empty(),
                version: self.relations.aux.version,
                incomplete_reasons: reasons,
            },
            attachments,
            cancelled: false,
        };
        self.discovery_done(op_id, caller, result, actions);
    }

    pub(crate) fn convergence_apply_discovery(
        &mut self,
        op_id: u64,
        caller: DiscoveryCaller,
        result: DiscoveryResult,
        actions: &mut Vec<Action>,
    ) {
        self.discovery_apply(op_id, caller, result, actions);
    }

    /// 应用发现结果：合并血缘、更新相关运行的成员与发现状态、通知 SA-b。
    pub(super) fn discovery_apply(
        &mut self,
        op_id: u64,
        _caller: DiscoveryCaller,
        result: DiscoveryResult,
        actions: &mut Vec<Action>,
    ) {
        if result.cancelled {
            return;
        }
        let mut changed = false;
        for edge in result.members.values() {
            let mut edge = edge.clone();
            if let Some(old) = self.relations.edges.get(&edge.child_thread_id) {
                if edge.agent_version == AgentVersion::Unknown {
                    edge.agent_version = old.agent_version;
                }
                if edge.can_accept_direct_input.is_none() {
                    edge.can_accept_direct_input = old.can_accept_direct_input;
                }
                edge.discovered_by = old.discovered_by;
                if *old == edge {
                    continue;
                }
            }
            self.relations
                .edges
                .insert(edge.child_thread_id.clone(), edge.clone());
            changed = true;
        }
        if changed {
            self.bump_version();
        }
        let results = &mut self.relations.aux.results;
        results.insert(op_id, result.clone());
        while results.len() > MAX_RESULTS {
            let first = *results.keys().next().expect("结果非空");
            results.remove(&first);
        }
        let runs: Vec<String> = self
            .relations
            .contexts
            .iter()
            .filter(|(run, c)| match &result.scope {
                DiscoveryScope::Run { run_id } => *run == run_id,
                DiscoveryScope::Subtree => {
                    c.identity.logical_root_thread_id == result.root
                        || c.identity.execution_thread_id == result.root
                }
            })
            .map(|(r, _)| r.clone())
            .collect();
        for run in &runs {
            for thread in result.members.keys() {
                self.refresh_member(run, thread, MemberChange::Discovery, actions);
            }
            let mut state = result.state.clone();
            for ((r, thread), classification) in &self.relations.aux.classifications {
                if r == run {
                    if let Membership::Pending(reason) = &classification.membership {
                        state.complete = false;
                        state
                            .missing
                            .entry(thread.clone())
                            .or_default()
                            .push(reason.clone());
                        if !state
                            .incomplete_reasons
                            .iter()
                            .any(|r| r == "unresolvedCandidate")
                        {
                            state.incomplete_reasons.push("unresolvedCandidate".into());
                        }
                    }
                }
            }
            if let Some(context) = self.relations.contexts.get_mut(run) {
                context.discovery = state;
            }
            self.touch_context(run, MemberChange::Discovery, actions);
        }
        for thread in result.members.keys() {
            self.persist_edge(thread, actions);
        }
        if changed {
            self.on_lineage_changed(&result.root, actions);
        }
    }

    /// 关闭时在途发现全部静默取消：不回调、迟到回包按编号丢弃。
    pub(super) fn discovery_shutdown(&mut self, actions: &mut Vec<Action>) {
        let ops: Vec<u64> = self.relations.aux.work.keys().copied().collect();
        for op_id in ops {
            self.relations.aux.work.remove(&op_id);
            self.relations.discoveries.remove(&op_id);
            actions.push(Action::Unschedule {
                key: crate::claim::TimerKey::DiscoveryDeadline(op_id),
            });
        }
    }
}

impl Machine {
    fn convergence_discovery_checks(&mut self, op_id: u64, actions: &mut Vec<Action>) -> bool {
        let op = &self.relations.discoveries[&op_id];
        let DiscoveryCaller::Converge { token, .. } = &op.caller else {
            return false;
        };
        if !self.convergence_token_current(token) {
            return true;
        }
        let generation = op.generation;
        let root = token.claim.thread_id.clone();
        let mut threads: BTreeSet<String> = self
            .manage_context(&token.run_id)
            .map(|c| c.members.keys().cloned().collect())
            .unwrap_or_default();
        let work = &self.relations.aux.work[&op_id];
        // ancestor/list 已按根过滤；loaded/list 的未知线程必须沿血缘验证。
        for (thread, edge) in work.found.iter().chain(work.read_edges.iter()) {
            let mut parent = edge.parent_thread_id.clone();
            for _ in 0..MAX_DEPTH {
                if parent == root {
                    threads.insert(thread.clone());
                    break;
                }
                let Some(edge) = work
                    .found
                    .get(&parent)
                    .or_else(|| work.read_edges.get(&parent))
                    .or_else(|| self.relations.edges.get(&parent))
                else {
                    break;
                };
                parent = edge.parent_thread_id.clone();
            }
        }
        let root_check = self.thread_capability(&root).is_none()
            && (!threads.is_empty()
                || self
                    .convergences
                    .ledgers
                    .get(&token.run_id)
                    .is_some_and(|l| l.work_epoch > 0));
        // 在传输包装 GoalRead 前建立版本；首次读取不能把“无版本”误判成竞态。
        for thread in &threads {
            if !self.relations.aux.candidate_versions.contains_key(thread) {
                self.candidate_changed(thread);
            }
            self.goal_fact_version(thread);
        }
        self.goal_fact_version(&root);
        if !self.relations.aux.candidate_versions.contains_key(&root) {
            self.candidate_changed(&root);
        }
        let work = self.relations.aux.work.get_mut(&op_id).unwrap();
        let root_goal = (root.clone(), "thread/goal/get".to_owned());
        if !work.checks_done.contains(&root_goal)
            && !work.checks_in_flight.contains(&root_goal)
            && !work.checks_queue.contains(&root_goal)
        {
            work.checks_queue.push_back(root_goal);
        }
        if root_check {
            let key = (root.clone(), "thread/read".to_owned());
            if !work.checks_done.contains(&key)
                && !work.checks_in_flight.contains(&key)
                && !work.checks_queue.contains(&key)
            {
                work.checks_queue.push_back(key);
            }
        }
        if threads.len() > MAX_MEMBERS {
            work.reasons.insert("tooManyMembers".into());
        }
        for thread in threads.into_iter().take(MAX_MEMBERS) {
            for method in ["thread/read", "thread/goal/get"] {
                let key = (thread.clone(), method.to_owned());
                if !work.checks_done.contains(&key)
                    && !work.checks_in_flight.contains(&key)
                    && !work.checks_queue.contains(&key)
                {
                    work.checks_queue.push_back(key);
                }
            }
        }
        while work.checks_in_flight.len() < DISCOVERY_READS {
            let Some((thread, method)) = work.checks_queue.pop_front() else {
                break;
            };
            work.checks_in_flight
                .insert((thread.clone(), method.clone()));
            work.candidate_versions
                .entry(thread.clone())
                .or_insert_with(|| {
                    self.relations
                        .aux
                        .candidate_versions
                        .get(&thread)
                        .copied()
                        .unwrap_or(0)
                });
            if method == "thread/goal/get" {
                work.check_goal_versions.insert(
                    thread.clone(),
                    self.stops.versions.get(&thread).copied().unwrap_or(0),
                );
            }
            let mut params = json!({"threadId": thread});
            if method == "thread/read" {
                // 根这里只核实输入能力，不读取父轮正文。
                params["includeTurns"] = json!(thread != root);
            }
            actions.push(Action::Request(RequestContext {
                resume_retried: false,
                stream_id: None,
                review_id: None,
                client: None,
                token: None,
                operation: Operation::Discovery { op_id, generation },
                method,
                params: Some(params),
            }));
        }
        !work.checks_in_flight.is_empty() || !work.checks_queue.is_empty()
    }

    fn convergence_discovery_check_reply(
        &mut self,
        op_id: u64,
        thread: &str,
        method: &str,
        result: Result<Value, Value>,
        actions: &mut Vec<Action>,
    ) {
        let work = self.relations.aux.work.get_mut(&op_id).unwrap();
        let key = (thread.to_owned(), method.to_owned());
        work.checks_in_flight.remove(&key);
        work.checks_done.insert(key);
        let run = match &self.relations.discoveries[&op_id].caller {
            DiscoveryCaller::Converge { token, .. } => token.run_id.clone(),
            _ => return,
        };
        // 根只核实能力和 goal；自己的新轮不会改变这两项，不能套用子候选版本。
        // goal 的独立版本仍在下方核验，连接与操作代际已在入口核验。
        if thread != self.relations.discoveries[&op_id].root
            && self.relations.aux.work[&op_id]
                .candidate_versions
                .get(thread)
                .copied()
                .unwrap_or(0)
                != self
                    .relations
                    .aux
                    .candidate_versions
                    .get(thread)
                    .copied()
                    .unwrap_or(0)
        {
            let work = self.relations.aux.work.get_mut(&op_id).unwrap();
            work.reasons.insert("candidateChanged".into());
            work.missing
                .entry(thread.into())
                .or_default()
                .push("candidateChanged".into());
            return;
        }
        let current_goal = method != "thread/goal/get"
            || self.relations.aux.work[&op_id]
                .check_goal_versions
                .get(thread)
                .copied()
                == Some(self.stops.versions.get(thread).copied().unwrap_or(0));
        let mut valid = false;
        if let Ok(value) = &result {
            if !current_goal {
                let work = self.relations.aux.work.get_mut(&op_id).unwrap();
                work.reasons.insert("goalChanged".into());
                work.missing
                    .entry(thread.into())
                    .or_default()
                    .push("goalChanged".into());
                return;
            }
            if method == "thread/goal/get" {
                if value.get("goal").is_some_and(Value::is_null)
                    || value
                        .get("goal")
                        .and_then(|g| g.get("status"))
                        .and_then(Value::as_str)
                        .is_some()
                {
                    if let Some(goal) = value.get("goal").filter(|g| g.is_object()) {
                        self.convergences
                            .goal_evidence
                            .insert(thread.into(), goal.clone());
                    }
                    let active = value
                        .get("goal")
                        .and_then(|g| g.get("status"))
                        .and_then(Value::as_str)
                        == Some("active");
                    self.record(thread).goal_active = active;
                    self.convergence_observe_goal(thread, active, actions);
                    self.record(thread).goal_status = value
                        .get("goal")
                        .and_then(|g| g.get("status"))
                        .and_then(Value::as_str)
                        .map(str::to_owned);
                    valid = true;
                }
            } else if thread == self.relations.discoveries[&op_id].root {
                if let Some(capability) = value
                    .get("thread")
                    .filter(|t| t.get("id").and_then(Value::as_str) == Some(thread))
                    .and_then(|t| t.get("canAcceptDirectInput"))
                    .and_then(Value::as_bool)
                {
                    self.convergences
                        .capabilities
                        .insert(thread.into(), (self.relations.conn_epoch, capability));
                    valid = true;
                }
            } else if let Some(info) = value
                .get("thread")
                .filter(|t| t.get("id").and_then(Value::as_str) == Some(thread))
            {
                let status = info
                    .get("status")
                    .and_then(|s| s.get("type"))
                    .and_then(Value::as_str);
                let turns = info.get("turns").and_then(Value::as_array);
                let inactive = matches!(status, Some("notLoaded" | "idle"))
                    && turns.is_some_and(|turns| {
                        turns.iter().all(|t| {
                            matches!(
                                t.get("status").and_then(Value::as_str),
                                Some("completed" | "failed" | "interrupted" | "errored")
                            )
                        })
                    });
                if self.relations.aux.candidate_observations.len() >= MAX_OBSERVED_THREADS
                    && !self
                        .relations
                        .aux
                        .candidate_observations
                        .contains_key(thread)
                {
                    if let Some(first) = self
                        .relations
                        .aux
                        .candidate_observations
                        .keys()
                        .next()
                        .cloned()
                    {
                        self.relations.aux.candidate_observations.remove(&first);
                    }
                }
                self.relations.aux.candidate_observations.insert(
                    thread.into(),
                    CandidateObservation {
                        conn_epoch: self.relations.conn_epoch,
                        inactive,
                    },
                );
                let historical_only = self.historical_subagent_for_run(&run, thread);
                if historical_only && inactive {
                    valid = true;
                }
                let verified_closed = self.convergences.closed_threads.contains(thread)
                    && self.convergences.observations.iter().any(|((t, _), o)| {
                        t == thread
                            && o.result == Some(crate::sa1b::ActivityResultStatus::Shutdown)
                            && o.conn_epoch == self.relations.conn_epoch
                    });
                if status != Some("notLoaded") || verified_closed {
                    if let Some(turns) = info.get("turns").and_then(Value::as_array) {
                        for turn in turns {
                            if let (Some(id), Some(status)) = (
                                turn.get("id").and_then(Value::as_str),
                                turn.get("status").and_then(Value::as_str),
                            ) {
                                self.convergence_observe_activity(thread, id, status, false);
                                let ended = matches!(
                                    status,
                                    "completed" | "failed" | "interrupted" | "errored"
                                );
                                self.observe_turn(thread, id, true, ended, actions);
                            }
                        }
                        valid = true;
                    }
                }
            }
        }
        if !valid {
            let work = self.relations.aux.work.get_mut(&op_id).unwrap();
            work.reasons.insert("activityUnknown".into());
            work.missing
                .entry(thread.into())
                .or_default()
                .push(method.into());
        }
        if thread != self.relations.discoveries[&op_id].root {
            self.refresh_member(&run, thread, MemberChange::Activity, actions);
        }
        self.convergence_sync_facts(&run);
    }
}

fn op_deadline_left(machine: &Machine, op_id: u64) -> u64 {
    machine
        .relations
        .discoveries
        .get(&op_id)
        .map_or(0, |o| o.deadline_ms.saturating_sub(machine.clock.mono_ms))
}
