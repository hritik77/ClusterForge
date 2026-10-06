# ClusterForge

ClusterForge is a small controller/worker cluster for accepting jobs over HTTP, tracking worker capacity, and assigning jobs to workers. The controller exposes a REST API to clients and a gRPC service to workers. Each worker exposes its own gRPC service for allocation, cancellation, and status requests.

> **Implementation status:** This project demonstrates job intake, in-memory queueing, placement, resource reservation, status tracking, cancellation, and DAG orchestration. The worker records assigned jobs but does not launch a workload process itself. Job, DAG, and run state are in memory and are lost when the controller restarts.

## Contents

- [Architecture](#architecture)
- [Job and node lifecycle](#job-and-node-lifecycle)
- [Scheduling and allocation](#scheduling-and-allocation)
- [DAG execution](#dag-execution)
- [Build](#build)
- [Run a local cluster](#run-a-local-cluster)
- [Configuration](#configuration)
- [REST API](#rest-api)
- [gRPC protocol](#grpc-protocol)
- [Logging and troubleshooting](#logging-and-troubleshooting)
- [Project layout](#project-layout)
- [Limitations and security](#limitations-and-security)

## Architecture

```text
                       HTTP/JSON
     Client  ------------------------------> Controller REST API
                                                  |
                                                  | in-memory jobs,
                                                  | queue and node state
                                                  v
                                           Job dispatcher
                                                  |
                         gRPC allocate/status     | gRPC register/heartbeat/
                     +----------------------------+-------------------------+
                     v                                                      v
               Worker node 1                                           Worker node 2
           allocation/status RPCs                                  allocation/status RPCs
```

### Controller

The controller process (`com.JobController.Discover`) hosts two services:

- **REST API:** accepts job submissions, returns job state, lists jobs, and requests cancellation.
- **Worker-facing gRPC API:** registers worker nodes, processes heartbeats, and accepts job status reports.

The controller maintains these in-memory structures:

- A node registry with each worker's advertised endpoint, reported state, and total/available CPU and memory.
- A job registry with job data, allocation state, and the node to which an allocated job was assigned.
- A bounded FIFO queue of pending job IDs.
- A cache of gRPC client channels to workers.

A single-threaded dispatcher drains the pending queue. It is triggered by new submissions, worker registration, heartbeats, and resource release after terminal job status or cancellation.

### Worker

Each worker (`com.Worker.Worker`) starts a gRPC server, registers itself with the controller, and sends a heartbeat every five seconds. The worker maintains an in-memory map of job records. On allocation it records the job as `ALLOCATED`; on cancellation it marks the job `CANCELLED`; it can also return the current job status to the controller.

There is no worker-side command or container launch in the current implementation. A `RUNNING` or `COMPLETED` state therefore requires a future execution integration or another status-reporting mechanism.

## DAG execution

The controller includes an in-memory DAG execution layer in `com.JobController.dag`. It validates registered DAG definitions, starts DAG runs, submits root tasks as ordinary ClusterForge jobs, and submits dependent tasks after all their dependencies report `COMPLETED`.

The DAG layer determines **which task is ready**. It submits tasks through `JobSubmissionService`; the existing `JobManager`, `JobQ`, and active scheduler continue to determine queueing and worker placement. DAG code does not reserve resources, select workers, or call worker gRPC directly. Independent ready tasks are submitted separately and may remain pending when the existing scheduler cannot currently place them.

Example internal use:

```java
DAG dag = new DAG("training", "training pipeline");
dag.addTask(new DAGTask("preprocess", 2, 4, Set.of()));
dag.addTask(new DAGTask("train", 4, 8, Set.of("preprocess")));

Discover.dagManager.registerDAG(dag);
DAGRun run = Discover.dagManager.startRun("training");
```

At run creation, root tasks begin `READY` and dependent tasks begin `BLOCKED`. Root tasks are then submitted as regular jobs and become `SUBMITTED`. On terminal job events, the DAG manager updates the corresponding task state. Completion may unlock dependent tasks; a failure retries the task up to its configured limit, then marks the run `FAILED`. Failed task state is not propagated to downstream tasks.

DAG runs are printed to the controller console as an ASCII adjacency view when task jobs are submitted and when task states change. Each task line shows its state and, once submitted, its ClusterForge job ID; outgoing arrows show dependent tasks. A job submitted through `POST /api/jobs` has no dependency information, so the controller displays it accurately as a standalone, one-node DAG rather than inventing edges.

DAG and run state is in memory. `POST /api/dags` accepts an explicit `CUSTOM_DAG` JSON specification, validates it, starts the run, and returns the graph, task states, and job IDs. `GET /api/dags/runs/{runId}` returns the current in-memory run view. There is no DAG cancellation endpoint. Job status events use the existing controller/worker protocol; workers are not DAG-aware and still do not launch workloads by themselves.

### BATCH DAG parsing

`com.JobController.job.parser.JobParserRegistry` includes the `BATCH` parser. A `BatchJobSpecification` is converted to exactly `count` independent tasks named `<name>-001`, `<name>-002`, and so on. Every generated task retains the same command and CPU/memory request; no dependency edges are added. This feature currently creates the DAG model only: it does not add a REST submission format, start a DAG run, or execute commands on workers.

The registry also includes a `MAP_REDUCE` parser. A `MapReduceJobSpecification` creates one independent `map-001` through `map-NNN` task per partition and a single `reduce` task depending on every map task. Mapper/reducer commands and their separate CPU/memory requests are retained on the tasks. The input path is retained on the specification; distributed splitting and data movement are not implemented.

The `PIPELINE` parser converts each supplied `PipelineTaskSpecification` directly into a DAG task, preserving its command, resource requests, and dependency set. It supports arbitrary DAG structures including multiple roots, joins, and diamonds. It checks basic task fields and duplicate task IDs; use `DAGValidator.validate(dag)` to reject missing dependencies and cycles. The parser only creates the graph and does not execute it.

The `PARAMETER_SWEEP` parser sorts parameter names and expands the Cartesian product of their string values into independent `experiment-NNN` tasks. Each task command appends the parameter flags in sorted order and receives the specification's CPU/memory request. Parameter maps and value lists are copied and exposed read-only; this parser does not run experiments or compare results.

The `CUSTOM_DAG` parser maps each explicitly specified task directly into the DAG model, preserving task IDs, commands, resources, and dependency order. It validates the completed graph with `DAGValidator`, so missing dependencies, self-dependencies, duplicate IDs, and cycles are rejected before a DAG is returned.

### DAG retries and worker loss

Each `DAGTask` has a `RetryPolicy`; existing constructors default to zero retries. `maxRetries` counts attempts after the first, and each retry creates a new ClusterForge job ID through the regular submission queue. Retry counts are stored per task in each `DAGRun`. Explicit job failures and worker-loss (`LOST`) events are retried according to the task policy. When retries are exhausted, the task and DAG run fail; no new dependent tasks are submitted, while already-submitted independent tasks are allowed to report their terminal state. Worker-loss handling releases the previous worker's resource reservation before notifying DAG listeners and submitting retries.

### Submit and inspect an explicit DAG

Use `POST /api/dags` to submit a custom DAG. The controller assigns a unique DAG ID and run ID; callers provide `jobType`, `name`, and `tasks`. Each task requires an `id`, `command`, positive integer `cpu` and `memory`, and an optional `dependsOn` string array (omit it or use `[]` for a root). The entire graph is validated before registration or job submission.

```sh
curl -i http://127.0.0.1:8080/api/dags \
  -H 'Content-Type: application/json' \
  -d '{
    "jobType": "CUSTOM_DAG",
    "name": "training-workflow",
    "tasks": [
      {"id":"download","command":"python download.py","cpu":2,"memory":4,"dependsOn":[]},
      {"id":"clean","command":"python clean.py","cpu":2,"memory":4,"dependsOn":["download"]},
      {"id":"train","command":"python train.py","cpu":8,"memory":16,"dependsOn":["clean"]},
      {"id":"test","command":"python test.py","cpu":4,"memory":8,"dependsOn":["clean"]},
      {"id":"deploy","command":"python deploy.py","cpu":2,"memory":4,"dependsOn":["train","test"]}
    ]
  }'
```

The `202 Accepted` response contains a `graph` string and a `tasks` array with dependencies, state, command, resources, and assigned `jobId` values. Save `runId` from the response and inspect it as tasks progress:

```sh
RUN_ID='paste-the-runId-from-the-submit-response'
curl -s "http://127.0.0.1:8080/api/dags/runs/$RUN_ID"
```

Root tasks are submitted immediately through the existing job queue; dependent tasks are submitted only after their dependencies report `COMPLETED`. Ensure workers are registered and have sufficient capacity. At present the worker records allocations but does not execute the command; DAG commands are metadata for the future execution layer. Invalid JSON, fields, dependencies, duplicate task IDs, self-dependencies, and cycles are rejected with `400 Bad Request`.

## Job and node lifecycle

### Job states

The protocol defines these job states:

| State | Meaning in this project |
| --- | --- |
| `PENDING` | Accepted by the controller and awaiting placement. |
| `ALLOCATED` | A worker accepted the allocation and the controller reserved resources. |
| `RUNNING` | Available in the protocol, but not set by the current worker implementation. |
| `COMPLETED` | Terminal; reported status releases the controller's resource reservation. |
| `FAILED` | Terminal; workload failure or allocation failure. |
| `CANCELLED` | Terminal; cancellation releases the reservation. |
| `LOST` | Terminal; the controller marked the job lost after its worker went down and released its reservation. |
| `HELD` | Defined in the protocol, but not currently used by the scheduler. |

Normal progression is `PENDING` → `ALLOCATED`. The protocol allows later states to be reported by workers; terminal states are `COMPLETED`, `FAILED`, and `CANCELLED`.

If a worker stops sending heartbeats, the controller marks its node `DOWN` after approximately 15 seconds without a heartbeat. The failure detector checks periodically (about every two seconds). Jobs currently assigned to that node are marked `LOST`, their reservations are released before DAG listeners are notified, and associated DAG tasks may retry according to their policy. Standalone jobs have no retry policy and are not automatically resubmitted.

### Node states

| State | Scheduling behavior |
| --- | --- |
| `AVAILABLE` | Eligible when the worker has enough available CPU and memory. |
| `NODE_ALLOCATED` | Not excluded by the scheduler; actual eligibility is based on state and remaining resources. |
| `MAINTENANCE` | Excluded from placement. |
| `DOWN` | Excluded from placement. |

For placement, a node must not be `DOWN` or `MAINTENANCE`, and must have at least the requested CPU **and** memory available.

## Scheduling and allocation

### Active policy: Least Loaded

Only `LeastLoadedSchedular` chooses the worker used for a real allocation. For each eligible node it calculates a score from current utilization and the job's normalized demand:

```text
score =
    usedCPU / totalCPU
  + usedMemory / totalMemory
  + requestedCPU / totalCPU
  + requestedMemory / totalMemory
```

The eligible node with the smallest score wins. Ties are resolved by ascending node ID. Resource availability is checked before scoring, so the selected node can satisfy both requested dimensions.

After selecting a node, the controller atomically reserves CPU and memory in its node tally, sends an allocation RPC, and marks the job `ALLOCATED` after the worker confirms success. If the worker rejects the request or the RPC fails, the controller releases the reservation and marks the job `FAILED`.

### Comparison-only policies

For an allocation attempt where Least Loaded finds an eligible node, the controller also evaluates three policies against the same pre-reservation candidate snapshot:

| Policy | Choice |
| --- | --- |
| First Fit | First eligible node in ascending node-ID order. |
| Best Fit | Eligible node minimizing the sum of normalized CPU and memory remaining after this job is placed. Node ID breaks ties. |
| Round Robin | Next eligible node in ascending node-ID order after the last node selected by this comparison scheduler; wraps to the first eligible node. |

These are **comparison results only**. Their selected nodes do not receive allocations and their choices do not affect the active Least Loaded policy. The Round Robin comparison cursor is advanced when it is evaluated, not only when a job ultimately succeeds.

On successful allocation, the controller prints a line similar to:

```text
Scheduling comparison for job 42: LeastLoaded=node 2, FirstFit=1, BestFit=2, RoundRobin=3
```

`none` means that a comparison policy found no eligible node. These logs are currently emitted to standard output and are not persisted as structured data.

### Queue behavior

The queue is FIFO for iteration, but a job that does not currently fit is left pending while the dispatcher continues checking later queued jobs. Consequently, a smaller job later in the queue may be allocated before an earlier job that needs more resources. When resources become available, the queue is scanned again.

Queue capacity is configurable. If it is full, submission is rejected; the job is not retained in the job registry. Successfully allocated jobs are removed from the queue. Jobs whose worker allocation RPC fails are marked failed and removed as well.

## Build

### Prerequisites

- Java 17 or newer
- Maven
- Network access to resolve Maven dependencies on the first build

Build the jar from the repository root:

```sh
mvn package
```

The Maven protobuf plugin generates Java message types and gRPC stubs from `src/main/proto/Broker.proto` during the build. The resulting application jar is `target/Architect-1.0-SNAPSHOT.jar`.

To launch with Maven-managed dependencies, create a runtime classpath file:

```sh
mvn dependency:build-classpath -Dmdep.outputFile=target/dependency-classpath.txt
```

## Run a local cluster

### 1. Build the project

```sh
mvn package
mvn dependency:build-classpath -Dmdep.outputFile=target/dependency-classpath.txt
```

### 2. Start the controller

In one terminal:

```sh
java -cp "target/Architect-1.0-SNAPSHOT.jar:$(cat target/dependency-classpath.txt)" \
  com.JobController.Discover
```

By default, the controller starts gRPC on port `9999` and REST on `127.0.0.1:8080`.

### 3. Start one or more workers

In another terminal on the same machine:

```sh
java -cp "target/Architect-1.0-SNAPSHOT.jar:$(cat target/dependency-classpath.txt)" \
  com.Worker.Worker
```

Workers default to 8 CPU units, 16 memory units, hostname `localhost`, worker port `9000`, and controller address `35.200.151.242:9999`.

For example, to run a worker on another machine:

```sh
java -Dclusterforge.worker.hostname=node-1 \
  -Dclusterforge.worker.host=10.0.0.21 \
  -Dclusterforge.worker.cpu=8 \
  -Dclusterforge.worker.mem=16 \
  -Dclusterforge.controller.host=35.200.151.242 \
  -cp "target/Architect-1.0-SNAPSHOT.jar:$(cat target/dependency-classpath.txt)" \
  com.Worker.Worker
```

`clusterforge.worker.host` is the address the controller will use to reach the worker; it is advertised as part of the registered node endpoint. It must be reachable **from the controller**. `clusterforge.controller.host` is the address the worker uses to connect to the controller and defaults to `35.200.151.242`. If multiple workers run on the same host, assign them distinct worker ports.

### 4. Submit a test job

From the controller host, submit to the default loopback REST listener:

```sh
curl -i http://127.0.0.1:8080/api/jobs \
  -H 'Content-Type: application/json' \
  -d '{"owner":"alice","description":"nightly batch","cpuRequested":2,"memRequested":4}'
```

To access REST remotely through `35.200.151.242`, start the controller with `-Dclusterforge.rest.host=0.0.0.0` and allow the REST port through the host/network firewall, then use `http://35.200.151.242:8080/api/jobs`. REST has no built-in authentication or TLS; do not expose it directly to an untrusted network. Prefer an authenticated TLS-terminating proxy.

The response should be `202 Accepted`. The worker and controller terminals show registration, allocation, and scheduler comparison log messages. See [REST API](#rest-api) for querying or cancelling jobs.

## Configuration

All settings are Java system properties supplied with `-Dname=value` before the main class.

| Property | Process | Default | Description |
| --- | --- | --- | --- |
| `clusterforge.controller.port` | Controller and worker | `9999` | Controller gRPC port. The worker reads this to connect to the controller. |
| `clusterforge.rest.host` | Controller | `127.0.0.1` | Local address on which the REST API binds. |
| `clusterforge.rest.port` | Controller | `8080` | REST API port. |
| `clusterforge.worker.port` | Worker | `9000` | Worker gRPC server port. |
| `clusterforge.worker.host` | Worker | `localhost` | Advertised worker host used to form the endpoint the controller calls. |
| `clusterforge.controller.host` | Worker | `35.200.151.242` | Controller host the worker connects to for registration, heartbeat, and status reports. |
| `clusterforge.worker.hostname` | Worker | `localhost` | Human-readable worker name registered with the controller. |
| `clusterforge.worker.cpu` | Worker | `8` | CPU capacity declared by the worker; must be positive. |
| `clusterforge.worker.mem` | Worker | `16` | Memory capacity declared by the worker; must be positive. |
| `clusterforge.queue.capacity` | Controller | `10000` | Maximum number of jobs waiting in the pending queue; must be positive. |

CPU and memory values are integer capacity units. The project treats them as abstract, consistent units: request values and worker declarations must use the same units. No conversion to cores, bytes, or another physical measure is performed.

## REST API

The REST API uses JSON generated from the protobuf `Job` message. Enum values are serialized as their enum names, for example `"PENDING"` and `"ALLOCATED"`. Successful responses include `Cache-Control: no-store` and `X-Content-Type-Options: nosniff`.

### Submit a job

```http
POST /api/jobs
Content-Type: application/json
```

Example:

```json
{
  "owner": "alice",
  "description": "nightly batch",
  "cpuRequested": 2,
  "memRequested": 4
}
```

`owner` and `description` must be non-blank. `cpuRequested` and `memRequested` must both be positive. Do not provide `id` or a non-default `state`; the controller assigns the ID and initializes the job as `PENDING`.

On acceptance, the response is `202 Accepted`, includes `Location: /api/jobs/{id}`, and contains the job JSON. Acceptance means the job was put in the in-memory queue; it does not mean that a worker has already accepted or begun the job.

### List jobs

```http
GET /api/jobs
```

Returns `200 OK` with a JSON array of known jobs sorted by ID. Jobs rejected because the queue was full are not included.

### Get a job

```http
GET /api/jobs/{id}
```

Returns `200 OK` with the job JSON, or `404 Not Found` if the ID is not known. IDs must be positive integers.

### Cancel a job

```http
DELETE /api/jobs/{id}
```

- A pending job is cancelled in the controller and removed from the queue.
- An allocated job is cancelled through the worker's gRPC service. The controller waits for an in-flight allocation to finish before deciding how to cancel.
- Successful cancellation and repeated cancellation return `200 OK` with the job JSON.
- A completed or failed job returns `409 Conflict` with the current job JSON.
- If the worker cannot confirm cancellation, the controller attempts to refresh the worker's job state; if cancellation remains unavailable, the API returns `503 Service Unavailable`.
- An unknown job returns `404 Not Found`.

### Error responses

Errors are JSON objects with an `error` string:

```json
{"error":"owner and description are required"}
```

| Status | When it is returned |
| --- | --- |
| `400 Bad Request` | Invalid job JSON, invalid ID, client-supplied ID/state, missing owner/description, or non-positive resources. |
| `404 Not Found` | Unknown route or job ID. |
| `405 Method Not Allowed` | Method not supported by the selected route. The `Allow` header lists permitted methods. |
| `413 Payload Too Large` | Request body exceeds 1 MiB. |
| `415 Unsupported Media Type` | Submission does not use `Content-Type: application/json`. |
| `503 Service Unavailable` | Queue is full or worker cancellation is unavailable. |

## gRPC protocol

The protobuf and service definitions are in `src/main/proto/Broker.proto`. Maven generates the Java code during the build.

### Worker-to-controller service

| RPC | Purpose |
| --- | --- |
| `registerNode(Node)` | Worker announces hostname, gRPC endpoint, and CPU/memory capacity. The controller assigns a node ID. |
| `heartBeat(Node)` | Worker refreshes liveness and reports its node state. Workers send these approximately every five seconds. |
| `reportJobStatus(Job)` | Worker reports a job-state update. Terminal reports release the controller's CPU and memory reservation and trigger another queue pass. |

### Controller-to-worker service

| RPC | Purpose |
| --- | --- |
| `allocate(AllocationCommand)` | Controller sends the job and an allocation ID. The worker records the job and responds with success/failure. |
| `cancelAllocated(JobRef)` | Controller asks the worker to cancel a previously allocated job. Cancellation is idempotent for jobs already cancelled. |
| `getJobStatus(Job)` | Controller queries the worker's current record for a job, primarily to reconcile a failed cancellation request. |

The gRPC channels use plaintext transport in the current implementation. Do not expose them to untrusted networks.

## Logging and troubleshooting

- **Worker does not register:** verify that the controller gRPC port is reachable from the worker and that `clusterforge.controller.host` and `clusterforge.controller.port` are correct.
- **Controller cannot allocate to a worker:** verify that the advertised `clusterforge.worker.host:clusterforge.worker.port` is reachable from the controller. A worker may be able to connect outbound to the controller even when its advertised inbound address is incorrect.
- **Job remains `PENDING`:** verify that at least one worker is registered, not `DOWN` or `MAINTENANCE`, and has enough available CPU and memory for the job. Pending jobs are retried on dispatch triggers; jobs that do not fit are retained.
- **Job becomes `FAILED` during allocation:** inspect controller output and worker logs for the allocation error. An allocation RPC error or worker rejection fails the job; a DAG task retries according to its policy, while a standalone job is not automatically resubmitted.
- **Node marked `DOWN`:** check worker liveness, network connectivity, and heartbeat logs. The controller uses a roughly 15-second heartbeat timeout.
- **Scheduler comparison output:** each successful allocation prints the active Least Loaded node and comparison-only First Fit, Best Fit, and Round Robin nodes. These lines are for observing policy differences; only Least Loaded controls allocation.
- **Port is already in use:** change the relevant controller REST/gRPC port or the worker port with the system properties in [Configuration](#configuration).

## Project layout

```text
src/main/java/com/JobController/
  Discover.java                 Controller entry point, node/job state, gRPC callbacks
  RestApiServer.java             HTTP/JSON job API
  JobManager.java                Pending queue dispatcher and worker allocation
  JobSubmissionService.java       Submission abstraction used by DAG execution
  JobEventListener.java           Terminal job lifecycle event contract
  JobQ.java                      Bounded in-memory FIFO of pending job IDs
  Scheduler.java                 Node-selection interface
  LeastLoadedSchedular.java      Active allocation policy
  FirstFitSchedular.java         Comparison-only policy
  BestFitSchedular.java          Comparison-only policy
  RoundRobinSchedular.java       Comparison-only policy
  SchedulerSupport.java          Shared node eligibility rules
  WorkerClientRegistry.java      Cached controller-to-worker gRPC clients
  dag/
    DAG.java                      In-memory DAG definition
    DAGTask.java                  Immutable task definition
    DAGRun.java                   Per-run task/job mapping and run state
    DAGRunState.java              DAG run lifecycle states
    DAGValidator.java             Validation and graph/state queries
    DAGManager.java                DAG registration and job lifecycle orchestration
src/main/java/com/Worker/
  Worker.java                    Worker gRPC server, registration and heartbeat
  JobMap.java                    Worker-side in-memory job records
src/main/proto/
  Broker.proto                   Messages, enums, and gRPC service definitions
```

## Limitations and security

- **In-memory only:** controller and worker records are not persisted. A restart loses the corresponding process's state. There is no durable queue, recovery protocol, or replicated controller.
- **No automatic retries:** jobs whose allocation fails or whose assigned worker is declared down are marked failed rather than retried elsewhere.
- **No workload execution:** workers currently record allocations and expose status/cancellation RPCs; they do not launch a process or container.
- **No authentication or encryption:** REST has no built-in authentication or TLS. gRPC channels are plaintext. Bind REST to loopback for local use, restrict service ports to trusted networks, and use a properly authenticated TLS-terminating proxy if REST must be accessed remotely.
- **Controller availability:** the controller is a single point of failure and owns the authoritative in-memory view of resources and jobs.
- **Resource accounting:** reservations are based on declared integer CPU and memory units, not live OS utilization. Capacity changes on a running worker are not dynamically discovered.
