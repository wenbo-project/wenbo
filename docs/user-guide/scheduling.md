# Scheduling Policies

The asynchronous Scheduler builds candidates from the ready queue, then calls the policy's `select(candidates, available_slots)`. Policies return ordered `SchedulingDecision` objects; Scheduler and WorkerPool handle hardware matching and lease acquisition.

## Built-in policies

| Name | Python class | Ordering |
| --- | --- | --- |
| `fifo` | `FIFOPolicy` | Preserve current candidate queue order |
| `sjf` | `ShortestJobFirstPolicy` | Ascending positive `estimated_tokens`; missing or nonpositive estimates last; ties use ready order |
| `priority` | `PriorityPolicy` | Descending DAG priority, then node priority, then ready order |
| `fair_rr` | `FairRoundRobinPolicy` | Select one node per DAG in round-robin order, following each DAG's first appearance among candidates |
| `fair_sjf` | `FairShortestJobFirstPolicy` | Round-robin across DAGs, with SJF ordering within each DAG |

`fair_rr` and `fair_sjf` regroup candidates on each `select` call and retain no cursor across scheduling rounds. They therefore provide no strict fairness guarantee across rounds. SJF uses structured token estimates rather than inferring costs from prompt length.

Create a policy with the factory function:

```python
from wenbo.scheduler.policy import build_policy

policy = build_policy("fair_sjf")
assert policy.policy_id == "fair_sjf@1"
```


Pass `policy=policy` when constructing Scheduler. Unknown policy names raise `ValueError`.

## Candidates and decisions

`SchedulingCandidate` contains DAG/node identifiers, priorities, a node reference, a token estimate, and `ready_order`. The latter is the queue position within the current candidate list, not a timestamp.

`SchedulingDecision` contains a candidate, policy name and version, score, and reason. List order determines which node is tried first for allocation. Scores explain individual policies and have no common scale across policies.

Candidates come only from READY nodes in RUNNING graphs and exclude nodes whose retry backoff has not expired. READY does not imply available hardware. A node waits if compatible workers are busy; if no registered capability supports it, the runtime marks it unschedulable.

## Extend policies

The following policy prioritizes nodes later in ready order. It illustrates the interface, not a recommended ordering:

```python
from wenbo.scheduler.policy import SchedulingPolicy
from wenbo.scheduler.types import SchedulingDecision


class NewestReadyPolicy(SchedulingPolicy):
    name = "newest_ready"
    version = "1"

    def select(self, candidates, available_slots):
        ranked = sorted(candidates, key=lambda item: item.ready_order, reverse=True)
        return [
            SchedulingDecision(
                candidate=item,
                score=float(item.ready_order),
                policy_name=self.name,
                policy_version=self.version,
                reason="later queue position first",
            )
            for item in ranked[:max(available_slots, 0)]
        ]
```

Custom policies should reference only input candidates, avoid duplicate selections, and neither change graph state nor reserve workers in advance. Use stable `name` and `version` values so [decision records](observability.md) distinguish implementation versions.

