Wenbo Documentation
===================

Wenbo is a DAG-based serving system for large-scale agent reasoning workloads.
It represents each agent task as a dynamic reasoning graph, incrementally
materializes executable DAG flows, and schedules ready nodes from all active
DAGs across shared GPU and CPU workers.

Traditional task-level serving binds each task to a fixed GPU-CPU resource
bundle even though agent execution alternates between GPU-intensive inference
and CPU-intensive tool execution, retrieval, and data processing. Wenbo
decouples individual computation steps from task-wide resource ownership,
enabling fine-grained resource sharing while preserving the dependencies
within each reasoning workflow.

Core Advantages:

* **Scalability:** Exposes parallelism both within a reasoning workflow and
  across concurrent workflows by scheduling fine-grained DAG nodes
  independently.
* **Adaptability:** Separates reasoning logic from resource placement and maps
  heterogeneous operations, including LLM inference, retrieval, tool use, and
  data processing, to appropriate GPU or CPU workers.
* **Verifiability:** Represents dependencies, intermediate results, and
  execution states explicitly, supporting tracing, provenance tracking,
  debugging, replay, and selective recovery.


.. toctree::
   :maxdepth: 1
   :caption: Introduction

   introduction

.. toctree::
   :maxdepth: 1
   :caption: Getting Started

   getting_started
