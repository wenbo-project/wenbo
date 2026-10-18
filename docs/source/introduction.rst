Introduction
============

What is Wenbo?
--------------

Wenbo is a DAG-based serving system designed to scale agent reasoning across
heterogeneous computing resources. Each agent task is represented as a
dynamic reasoning graph whose nodes describe atomic computation steps, such
as LLM inference, tool invocation, retrieval, verification, and data
processing. Edges express the dependencies between those steps.

Unlike a static workflow, the complete reasoning graph does not need to be
known before execution begins. Wenbo evolves the graph as intermediate model
outputs and tool results become available, incrementally materializing the
resolved portions into executable DAG flows. Ready DAG nodes can then be
scheduled independently across a shared pool of GPU and CPU workers.

Why Wenbo?
----------

Existing agent systems commonly use task-level resource ownership: each task
receives a fixed GPU-CPU bundle and retains both resources for its entire
lifetime. This model is inefficient because agent execution alternates
between GPU-intensive LLM inference and CPU-intensive orchestration,
retrieval, tool execution, and data processing. While one phase runs, part of
the reserved bundle can remain idle, creating resource fragmentation and
limiting cluster-wide utilization.

Wenbo replaces task-level ownership with node-level scheduling. All ready
nodes from all active DAGs enter a global scheduling domain and are dispatched
to compatible idle workers as soon as their dependencies are satisfied. GPU
and CPU work can therefore progress independently across different tasks,
allowing otherwise idle capacity to serve another workflow.

The achievable throughput gain depends primarily on the alignment between the
system's GPU/CPU resource ratio and each task's GPU/CPU workload ratio. The
largest gains occur when the more abundant resource also carries more of the
workload; if the scarce resource is already the bottleneck, DAG-level
scheduling provides limited additional benefit.

Architecture
------------

.. image:: _static/wenbo_architecture.png
   :alt: Wenbo architecture showing the Director, Scheduler, and Workers
   :width: 100%
   :align: center

Wenbo separates reasoning orchestration, global scheduling, and hardware
execution into three primary components:

.. list-table::
   :header-rows: 1
   :widths: 24 76

   * - Component
     - Responsibility
   * - Director
     - Acts as the reasoning control plane. It constructs the initial
       reasoning graph, maintains task context, incrementally materializes
       executable DAG flows, and evolves the graph in response to execution
       results.
   * - Scheduler
     - Maintains a global view of executable nodes and DAG state, maps each
       ready node to a suitable GPU or CPU worker, monitors execution, and
       communicates intermediate state back to the Director.
   * - Workers
     - Execute individual DAG nodes and manage local resources such as CPUs,
       GPUs, memory, models, caches, and runtime environments. They report
       outputs, execution status, utilization, timing, and failures to the
       Scheduler.

The Director materializes executable work from the current reasoning state.
The Scheduler dispatches dependency-free nodes to compatible Workers without
reserving a fixed worker bundle for the whole task. Each completion releases
the Worker, updates the DAG state, unlocks eligible successors, and may cause
the Director to refine the reasoning graph. This feedback loop continues as
the agent reasoning process evolves.

Core Capabilities
-----------------

Wenbo provides the following system capabilities:

* **Dynamic DAG Generation:** Builds and continuously refines reasoning graphs
  as intermediate outputs reveal new steps, branches, or termination
  conditions.
* **Incremental Materialization:** Converts the currently resolved portion of
  a reasoning graph into executable DAG flows without requiring a complete
  execution plan in advance.
* **Global Dependency-Aware Scheduling:** Discovers ready nodes across all
  active DAGs and schedules them while preserving every intra-task
  dependency.
* **Heterogeneous Node Execution:** Places GPU-intensive inference on GPU
  workers and CPU-oriented orchestration, retrieval, tools, and data
  processing on CPU workers.
* **Fine-Grained Parallelism:** Exploits independent branches within one agent
  workflow as well as parallelism across multiple concurrent workflows.
* **Elastic Fault Tolerance:** Reschedules retryable nodes after Worker
  failures while preserving completed nodes and intermediate results, avoiding
  a full DAG restart when recovery state is available.
* **Execution Verifiability:** Maintains explicit dependencies, intermediate
  results, execution states, and runtime feedback to support provenance,
  tracing, debugging, replay, and selective recovery.
