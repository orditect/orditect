# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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