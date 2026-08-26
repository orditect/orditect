# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.1] - TBD

Multi-parent dependency governance (passive protocol-layer API).

### Added
- `DependencyGovernor` (orditect.flow.governance): register_dependency /
  get_ready_tasks / vote_cancel / notify_task_terminal /
  get_dependency_graph / result_consumed / invalidate_exempt_snapshot.
  Governance only: never creates tasks, never schedules execution;
  readiness is driven exclusively by external notify_task_terminal() calls.
  Vocabulary neutrality (T6): success / terminal / ready words are
  caller-declared at construction.
- Parent classification at registration: non-terminal parents are counted
  and added to active_children; terminal-success parents are ignored;
  terminal-abnormal parents count as already-cast cancel votes. Success
  never auto-votes (prevents accidental cancellation); abnormal terminals
  auto-vote (hang prevention). Threshold voting is atomic — exactly one
  concurrent voter triggers cancellation.
- Exemption snapshot: frozen at registration (explicit list capped at 10,
  or inherited along the primary-parent chain); the executor prefers the
  snapshot over the live ancestor walk when present, with graceful
  fallback. invalidate_exempt_snapshot resets it on reopen;
  RecoveryService's rerun path invokes it when a governor is injected.
- Offline tools: scan_dependency_cycles (full-graph DFS, line 2 of cycle
  detection) and rebuild_dep_counters (admin recovery from the cold store).
- Injection points: TaskOrchestrator(dependency_governor=...),
  RecoveryService(dependency_governor=...). Not injected: every new code
  path is inert.

### Changed
- Requires orditect-core >= 0.1.1 (dependency-governance primitives).
- executor: exemption determination gains a snapshot-first pre-branch;
  _find_ancestor_resources unchanged.

## [0.1.0] - TBD

First release as orditect-flow (renamed from fastapi-taskflow). Introduces
the recovery plane: breakpoint-resume, mid-point custom replay, and the
snapshot data foundation for time-travel and visualization.

### Added
- Renamed from fastapi-taskflow to orditect-flow (namespace package migration).
- Dependencies on orditect-core (reopen primitive) and orditect-protocol
  (snapshot / content / audit domain contracts).
- **Snapshot sink injection point (F2)**: executor writes execution snapshots
  at lifecycle points (running / terminal) to the protocol snapshot domain.
  Default NullSink = zero cost, zero behavior change. Observation
  non-blocking (T9); execution_id read from the core hot record (T11);
  terminal writes use save_terminal (T3).
- **Result reuse short-circuit (F3)**: a node whose latest generation already
  reached a caller-declared success word reuses its result from the core hot
  record instead of re-executing. Capability degradation is explicit (T8).
  Vocabulary neutrality — success words caller-declared (T6).
- **RecoveryService (F4)**: resume / rerun over the recursive task tree.
  Per-node decision algorithm (reuse vs rerun); rerun bypasses submit and
  dispatches executor.execute directly after core reopen_task. task_factory
  injected by the caller (mechanism to the framework, semantics to the
  business). One node's failure does not block the rest of the tree.

### Changed
- (none — zero behavior change commitment; all recovery features default off)

### Design Decision
- reopen is a new-generation primitive (core), not a state transition;
  terminal protection (T3) remains unconditional within one generation.
- Recovery dispatch bypasses submit (reopen already reset state); executor
  drives re-execution directly.
- Pause/suspend semantics are deferred to a dedicated v0.2.0 iteration
  (vocabulary-neutral suspend mechanism + executor suspend/resume path).