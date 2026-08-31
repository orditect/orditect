# Orditect Dependency-Governance Example

Multi-parent fan-in with **zero infrastructure**: C runs only after A
AND B finish — the case a linear pipeline (and recursive composition)
cannot express. Demonstrates the full `DependencyGovernor` lifecycle:
register -> notify -> readiness -> voting, plus the voting discipline
(failed parents auto-vote; all-parents-failed cancels the child) and the
two observability views (dependency graph vs snapshot tree) that must
not be conflated.

Key point: the governor is **passive** — it never creates tasks and
never schedules execution. The demo wires `notify_task_terminal` at its
task-closure points because that is the caller's contract (Ch.8.5).

## Run

    pip install -r requirements.txt
    python run_demo.py

For the full chapter, see `examples/README.md` Ch.8.