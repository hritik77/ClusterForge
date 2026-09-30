# ClusterForge

ClusterForge is a small controller/worker cluster for accepting jobs over HTTP, tracking worker capacity, and assigning jobs to workers. The controller exposes a REST API to clients and a gRPC service to workers. Each worker exposes its own gRPC service for allocation, cancellation, and status requests.

> **Implementation status:** This project currently demonstrates job intake, in-memory queueing, placement, resource reservation, status tracking, and cancellation. The worker records assigned jobs but does not launch a workload process itself. Job execution and durable cluster state are not implemented.

## Contents

- [Architecture](#architecture)
- [Job and node lifecycle](#job-and-node-lifecycle)
- [Scheduling and allocation](#scheduling-and-allocation)
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

## Job and node lifecycle

### Job states

The protocol defines these job states:

| State | Meaning in this project |
| --- | --- |
| `PENDING` | Accepted by the controller and awaiting placement. |
| `ALLOCATED` | A worker accepted the allocation and the controller reserved resources. |
| `RUNNING` | Available in the protocol, but not set by the current worker implementation. |
| `COMPLETED` | Terminal; reported status releases the controller's resource reservation. |
| `FAILED` | Terminal; reported status or worker failure releases the reservation. |
| `CANCELLED` | Terminal; cancellation releases the reservation. |
| `HELD` | Defined in the protocol, but not currently used by the scheduler. |

Normal progression is `PENDING` → `ALLOCATED`. The protocol allows later states to be reported by workers; terminal states are `COMPLETED`, `FAILED`, and `CANCELLED`.

If a worker stops sending heartbeats, the controller marks its node `DOWN` after approximately 15 seconds without a heartbeat. The failure detector checks periodically (about every two seconds). Jobs currently assigned to that node are marked `FAILED` and their reservations are released. They are **not** automatically requeued or retried.

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
- **Job becomes `FAILED` during allocation:** inspect controller output and worker logs for the allocation error. An allocation RPC error or worker rejection fails the job; the controller does not retry it automatically.
- **Node marked `DOWN`:** check worker liveness, network connectivity, and heartbeat logs. The controller uses a roughly 15-second heartbeat timeout.
- **Scheduler comparison output:** each successful allocation prints the active Least Loaded node and comparison-only First Fit, Best Fit, and Round Robin nodes. These lines are for observing policy differences; only Least Loaded controls allocation.
- **Port is already in use:** change the relevant controller REST/gRPC port or the worker port with the system properties in [Configuration](#configuration).

## Project layout

```text
src/main/java/com/JobController/
  Discover.java                 Controller entry point, node/job state, gRPC callbacks
  RestApiServer.java             HTTP/JSON job API
  JobManager.java                Pending queue dispatcher and worker allocation
  JobQ.java                      Bounded in-memory FIFO of pending job IDs
  Scheduler.java                 Node-selection interface
  LeastLoadedSchedular.java      Active allocation policy
  FirstFitSchedular.java         Comparison-only policy
  BestFitSchedular.java          Comparison-only policy
  RoundRobinSchedular.java       Comparison-only policy
  SchedulerSupport.java          Shared node eligibility rules
  WorkerClientRegistry.java      Cached controller-to-worker gRPC clients
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
