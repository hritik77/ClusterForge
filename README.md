# ClusterForge

The controller exposes its worker gRPC service on port `9999` and a REST API on `127.0.0.1:8080`. The REST listener binds to loopback by default. Configure `clusterforge.rest.host` and `clusterforge.rest.port` with JVM system properties to change its address. For remote access, put the API behind an authenticated TLS-terminating reverse proxy and restrict direct access to the service port. The REST API itself does not provide authentication or TLS. Worker gRPC traffic also uses plaintext; keep the controller and worker ports on a trusted network.

The controller and workers keep jobs and node registrations in memory; this implementation does not persist or recover cluster state across process restarts. Job IDs are unique only for the lifetime of a controller process.

## Running

Build with Java 17 and Maven, then start the controller and workers in separate terminals:

```sh
mvn package
mvn dependency:build-classpath -Dmdep.outputFile=target/dependency-classpath.txt
java -cp "target/Architect-1.0-SNAPSHOT.jar:$(cat target/dependency-classpath.txt)" \
  com.JobController.Discover
```

Start a worker with:

```sh
java -cp "target/Architect-1.0-SNAPSHOT.jar:$(cat target/dependency-classpath.txt)" \
  com.Worker.Worker
```

Override worker capacity and network settings with JVM properties, for example `-Dclusterforge.worker.hostname=node-1 -Dclusterforge.worker.host=10.0.0.21 -Dclusterforge.worker.cpu=8 -Dclusterforge.worker.mem=16 -Dclusterforge.controller.host=10.0.0.10`. The controller must be able to reach each worker's advertised `clusterforge.worker.host:clusterforge.worker.port` endpoint.

## Jobs API

Submit a job using the existing `Job` JSON representation. The controller assigns `id` and `state`; omit those fields from the request:

```http
POST /api/jobs
Content-Type: application/json

{
  "owner": "alice",
  "description": "nightly batch",
  "cpuRequested": 2,
  "memRequested": 4
}
```

The controller allocates the job immediately to a worker with sufficient capacity. A successful submission returns `201 Created`, the job JSON, and a `Location` header. If no worker can accept it, the API returns `503 Service Unavailable`; jobs are not queued.

| Method | Path | Result |
| --- | --- | --- |
| `POST` | `/api/jobs` | Submit a job; the controller assigns its ID and initial state. |
| `GET` | `/api/jobs` | List known jobs, sorted by ID. |
| `GET` | `/api/jobs/{id}` | Return one job, or `404 Not Found`. |
| `DELETE` | `/api/jobs/{id}` | Cancel a job. Repeated cancellation is successful; completed or failed jobs return `409 Conflict`. Worker communication failures return `503 Service Unavailable`. |

Submission requires non-empty `owner` and `description`, and positive `cpuRequested` and `memRequested` values. IDs and states cannot be supplied by clients. Invalid JSON or input returns `400 Bad Request`; errors use a JSON body with an `error` field. Requests are limited to 1 MiB.
