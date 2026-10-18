Getting Started
===============

This guide installs Wenbo from source and runs the self-contained quickstart.
The example uses a mock execution backend, so it does not require an API key,
a model download, or access to GPU hardware.

Requirements
------------

Before installing Wenbo, make sure the following tools are available:

* Python 3.10 or newer
* Git
* ``pip``

Using an isolated Python environment is recommended. The following example
uses Conda with Python 3.11:

.. code-block:: console

   conda create -n wenbo python=3.11 -y
   conda activate wenbo

Installation
------------

Clone the repository and install Wenbo in editable mode:

.. code-block:: console

   git clone https://github.com/wenbo-project/wenbo.git
   cd wenbo
   python -m pip install -e .

Editable installation makes the local ``wenbo`` package available to the
examples while keeping source changes immediately visible in the environment.

Run the Quickstart
------------------

From the repository root, run:

.. code-block:: console

   python examples/quickstart.py

The example loads ``examples/dags/quickstart.json`` and submits its CPU and GPU
nodes to the Wenbo scheduler. ``MockBackend`` simulates node execution, so the
example validates the scheduling path without invoking a real model.

Verify the Result
-----------------

A successful run ends with output similar to:

.. code-block:: text

   Wenbo quickstart finished: completed=1, failed=0, output=.local/quickstart/result.json

The complete result is written to ``.local/quickstart/result.json``. It
contains run metadata, DAG and node states, the execution timeline, and the
scheduler's decisions. To inspect the formatted JSON, run:

.. code-block:: console

   python -m json.tool .local/quickstart/result.json

Optional Configuration
----------------------

Use ``--policy`` to select one of the scheduling policies exposed by the
quickstart:

.. code-block:: console

   python examples/quickstart.py --policy priority

Available values are ``fifo``, ``priority``, ``fair_rr``, and ``sjf``.

The mock backend can also simulate execution latency. For example:

.. code-block:: console

   python examples/quickstart.py --mock-mode estimated --base-latency-ms 20

Run the built-in help command to see every supported option:

.. code-block:: console

   python examples/quickstart.py --help
