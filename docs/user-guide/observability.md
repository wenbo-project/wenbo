# Events, Timelines, and Tracing

The runtime provides lifecycle events, node timelines, policy decisions, and standardized traces. Each serves a different observation need; interpret time fields using their respective clocks.

## Clocks and units

| Record | Time field | Clock and unit |
| --- | --- | --- |
| `Event` | `timestamp` | `time.time()`, Unix timestamp in seconds |
| Runtime timeline / decisions | `time` | `time.monotonic()`, seconds |
| Runtime trace | `timestamp_s` | Monotonic clock, seconds |

Monotonic clocks support elapsed-time comparisons within the same run and cannot be converted to calendar dates. Do not subtract timestamps from different clocks. See [Experiments](../experiments/real-datasets.md) for the separate benchmark's virtual-time metrics.

## Subscribe to events

```python
from wenbo.runtime.event import EventBus, EventType

bus = EventBus()


async def report_completed(event):
    print(event.dag_id, event.node_id, event.data)


bus.subscribe(EventType.NODE_COMPLETED, report_completed)
```

Pass the same bus to Scheduler and StateTracker. Publishing awaits current subscribers concurrently. Ordinary handler exceptions are logged rather than propagated directly to the publisher. Do not rely on subscription order to coordinate state transitions. Duplicate subscriptions produce duplicate callbacks; `unsubscribe()` removes one matching subscription per call.

## Read runtime records

| Scheduler attribute | Contents |
| --- | --- |
| `timeline` | Node started / finished milestones recorded by Coordinator |
| `decisions` | Policy name, version, order, score, reason, and worker ID when available |
| `trace` | Standardized node, DAG, resource occupancy, and release events |
| `trace_summary` | Event counts, time bounds, and DAG completion latency statistics |
| `stats` | Scheduling rounds, candidate and decision counts, and host scheduling overhead |
| `running_by_hardware` | Current task counts by worker hardware type |


Completion latency pairs matching `dag.arrived` and `dag.completed` events and is measured in seconds. Unfinished or failed graphs, and graphs without an arrival event, are excluded from latency samples. This metric does not include external request time before queuing.

## Trace retention modes

`SchedulingTrace` can be used independently; callers select a clock label for their timestamps. The example below supplies virtual timestamps manually.

| Mode | Individual events | In-memory `events` | Use case |
| --- | --- | --- | --- |
| `summary` | Not retained | Empty list | Counts and completion latency statistics only |
| `full` | Retained in memory | All serialized events | Small runs and event-by-event inspection |
| `stream` | Written to JSONL | Empty list | Retain full events with less memory used by event objects |

All three modes retain completion time / latency information for statistics, so `summary` does not imply strictly constant memory. The asynchronous Scheduler currently uses `full` mode through TimelineCollector and exposes no constructor option to switch modes directly.

## Write JSONL

The following example runs independently:

```python
from wenbo.runtime.trace import SchedulingTrace, TraceEventType

trace = SchedulingTrace(
    runner="demo",
    clock="virtual",
    mode="stream",
    stream_path=".local/docs-trace/events.jsonl",
)
try:
    trace.record(TraceEventType.DAG_ARRIVED, 0.0, "demo")
    trace.record(TraceEventType.DAG_COMPLETED, 1.0, "demo")
    trace.close()
except BaseException:
    trace.abort()
    raise

assert trace.summary["dag_completion_latency_mean_s"] == 1.0
print(trace.summary["event_count"])
```

Event times must be finite and nondecreasing. Stream mode requires `stream_path`; other modes reject it. Output `record_type` values are `trace.header`, `trace.event`, and `trace.footer`. Header/footer `schema_version` is `wenbo.scheduling-trace.v1`. Successful `close()` atomically replaces the destination with the temporary file; `abort()` discards the incomplete temporary file. Close on success and abort on exceptions.
