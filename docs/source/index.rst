Wenbo Documentation
===================

Wenbo is an event-driven scheduling runtime for multi-agent reasoning
workloads. It represents each workload as a directed acyclic graph (DAG),
tracks node dependencies, and schedules ready work from all active DAGs
through one shared execution path.

Instead of binding an entire agent workflow to a fixed set of resources,
Wenbo schedules each ready node on a compatible CPU or GPU worker. This keeps
workflow dependencies intact while allowing heterogeneous resources to serve
multiple workloads efficiently.

Core Advantages:

* **Global Node-Level Scheduling:** Collects ready nodes from all active DAGs
  into one scheduling domain, allowing resources to be shared across
  concurrent reasoning workloads.
* **Efficient CPU/GPU Utilization:** Schedules each node on a compatible
  worker, so one workflow can use capacity that another workflow temporarily
  leaves idle.
* **Dependency-Aware Execution:** Preserves DAG dependency guarantees while
  independently dispatching ready work according to resource availability.
* **Pluggable Scheduling Policies:** Supports multiple scheduling strategies
  without coupling workflow definitions to a specific execution policy.


.. toctree::
   :maxdepth: 1
   :caption: Introduction

   introduction
