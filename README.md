# Goal

Build a patient events write platform that provides strict idempotency on admission, authoritative PHI versioning, and an immutable downstream De-identified (De-ID) record materialized from a CDC commit stream. De-ID payloads and Kafka messages must exclude PHI.

Targets: Throughput: 1,000,000 writes/min (~16,667/sec) at ~20KB/request. Latency: p99 <= 5s admission -> De-ID readable-by-key (healthy). Safety: no PHI fields in Kafka payloads or De-ID stores; separation is enforced by connector filtering and downstream redaction. Correctness: authoritative truth is PHI head + immutable PHI versions + CDC replay; Redis receipts improve client UX and reporting but are not correctness-critical.


### Example if new Patient record:
#### Patient Record
- Name  
- DOB  
- Favorite Color  
#### Create Two Records
##### → PHI Patient Record
- PHI Record ID  
- Name  
- DOB  
- Favorite Color  
##### → De-identified Patient Record
- Obfuscated id which is a reference to PHI Patient Record  
- Favorite Color  

Full Solution:    
- [Architecture](./ARCHITECTURE.md) 
- [Trade Offs](./TRADEOFFs.md) 
- [Testing Guide](./docs/TESTING.md)

### AI Coding Outline Prompt:
- `ARCHITECTURE.md` is the authoritative design and invariants  
- “Any code changes must conform to it”  
- Stack summary (Flink Python, Java REST, Kafka, Postgres, containers)

## Local Runbooks

### Infra Build, Start, Stop, Restart, Read Logs
- **Postgres** 
- **Kafka** with Kafka init topics
- **Kafka Connect** with Debezium connectors registered
- **Flink JobManager, TaskManager, and Job Submitter** (deid_projection_job.py submitted automatically)
- **Redis** for idempotency
- **API** service (Java REST) + **Liquibase**

#### Build

##### Flink Image
```bash
docker compose -f infra/docker-compose.yml build
```

##### API Jar
```bash
mvn -q -f services/pom.xml clean package
```

##### Python .venv
```bash
python3 -m venv .venv
source ./.venv/bin/activate
python3 -m pip install aiohttp redis\
```

#### Start
```bash
# Note DEBUG=false/true will turn on debugging in the flink script. 
docker compose -f infra/docker-compose.yml up -d # -d means "detached mode"
sleep 10
python3 infra/scripts/validate_e2e.py # Validate all infra is running
```


#### Stop
```bash
docker compose -f infra/docker-compose.yml down -v # -v means "shorthand for --volumes
```

#### Restart ( /w build )
```bash
# Resources
## postgres, redis, kafka, kafka-connect, flink-jobmanager, flink-taskmanager, api
## flink-submit, topic-init, connector-init 
resource=?
docker compose -f infra/docker-compose.yml up -d --build --force-recreate ${resource}
```

#### Get Logs
```bash
resource=?
docker compose -f infra/docker-compose.yml logs ${resource}
```

### Run Test

#### Flink Projection Unit Tests (automated through .github/workflows/test.yaml)

```bash
# Run with direct module reference
python3 -m unittest infra/flink/tests/test_deid_projection_job.py -v

# Or discover and run from tests directory
python3 -m unittest discover -s infra/flink/tests -p "test*.py" -v
```
#### API Unit Tests (automated through .github/workflows/services-tests.yaml)

```bash
mvn -f services/pom.xml test
```

#### API Load Test Script
Note: 1M Records a minute / 16 shards / 60 seconds = 1,041 records/sec per shard and 99% of them are updates.
```bash
# Example creates 82400 records and will take about 4 minutes to run
python3 infra/scripts/inject_phi_load.py --inserts-per-second 1000 --num-inserts 1 --concurrency 200 --update-ratio 100
# Output shows API ingestion rate
```

Notes:
- `--num-inserts` is treated as duration in seconds.
- Total inserts = `inserts-per-second * num-inserts`.
- `--update-ratio` supports decimals and large values (e.g. `0.2`, `1`, `1000`).
- Updates run only after all inserts complete, using captured `phi_id`s.



### Database Records

### Login
```bash
docker compose -f infra/docker-compose.yml exec postgres psql -U app -d patient_events # access db
```

#### Default Queries

```sql
SELECT de_id FROM phi_patient_head WHERE phi_id = '8cea0cba-5558-49e5-b943-095acaaadd1f';
SELECT count(*) from phi_patient_versions;SELECT count(*) from deid_patient_versions;
```

#### Latency Queries for the Persisted Data
Query the `deid_projection_latency` from Phase 1 to Phase 2 - de_id persistence:
```sql
-- Query by de_id to see latency for a specific record
SELECT de_id, phi_id, current_version, persisted_version, 
       phi_created_at, deid_created_at, latency_ms
FROM deid_projection_latency
WHERE de_id = 'abcd1234-ef56-78gh-ijkl-mnopqr000000'
ORDER BY current_version DESC;

-- Calculate average latency for records where persisted_version = current_version (fully confirmed)
SELECT 
  COUNT(*) as confirmed_count,
  ROUND(AVG(latency_ms)::numeric, 2) as avg_latency_ms,
  ROUND(MIN(latency_ms)::numeric, 2) as min_latency_ms,
  ROUND(MAX(latency_ms)::numeric, 2) as max_latency_ms,
  ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY latency_ms)::numeric, 2) as p95_latency_ms,
  ROUND(PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY latency_ms)::numeric, 2) as p99_latency_ms
FROM deid_projection_latency
WHERE persisted_version = current_version 
  AND deid_created_at IS NOT NULL;

-- Count pending de-identifications (where persisted_version < current_version)
SELECT COUNT(*) as pending_count
FROM deid_projection_latency
WHERE persisted_version < current_version;
```


### Access Kafka

#### Show Topics
```bash
docker compose -f infra/docker-compose.yml exec kafka /usr/bin/kafka-topics --bootstrap-server kafka:9092 --list
```

#### Show Partitions
```bash
docker run --rm -it --network infra_default edenhill/kcat:1.7.1  -b kafka:9092 -L        
```

#### Show data in the Kafka Topics
```bash
# Check size
docker run --rm -it --network infra_default edenhill/kcat:1.7.1  -b kafka:9092 -t phi_patient_versions -C -o end -e
docker run --rm -it --network infra_default edenhill/kcat:1.7.1  -b kafka:9092 -t deid_dlq -C -o end -e
docker run --rm -it --network infra_default edenhill/kcat:1.7.1  -b kafka:9092 -t phi_patient_versions_deletes -C -o end -e

# Look at the last record of partition 0
docker run --rm -it --network infra_default edenhill/kcat:1.7.1  -b kafka:9092 -t phi_patient_versions -C -o -1 -e -p 0 -f 'Key: %k | Partition: %p | Offset: %o\nValue: %s\n\n'
docker run --rm -it --network infra_default edenhill/kcat:1.7.1  -b kafka:9092 -t deid_dlq -C -o -1 -e -p 0 -f 'Key: %k | Partition: %p | Offset: %o\nValue: %s\n\n'
docker run --rm -it --network infra_default edenhill/kcat:1.7.1  -b kafka:9092 -t phi_patient_versions_deletes -C -o -1 -e -p 0 -f 'Key: %k | Partition: %p | Offset: %o\nValue: %s\n\n'
```

#### Publish bad data in the Kafka Topics to test DLQ(phi_id != ph_id)
```bash
echo '8a1054b7-bc2c-4d36-a48f-29c01cc2602b|{"ph_id":"99c723b2-4df2-4c9d-88bf-cec5a6f2e062","de_id":"8a1054b7-bc2c-4d36-a48f-29c01cc2602b","event_id":"ac3be3d6-8de2-4a7f-bfbf-137eca16c95e","request_hash":"a32190bff6db025c6c698658e642696cce00a23820630d89159a1b331e9fa4fb","version":101,"favorite_color":"yellow","created_at":1771535697850933}' \
| docker run --rm -i --network infra_default edenhill/kcat:1.7.1 \
  -b kafka:9092 \
  -t phi_patient_versions \
  -P \
  -K '|'
```

### Access Redis
#### Show ALL Keys
```bash
docker compose -f infra/docker-compose.yml exec redis redis-cli KEYS "*"
```

#### Fetch Idempotency Key (with Timestamps)
```bash
# Fetch a key from Redis (with auto-detection of idempotency: prefix)
python3 infra/scripts/fetch_redis.py 0782c9b6-aed5-477d-82ce-8f2bf8e12683
```

#### Calculate Redis Latency Statistics
```bash
# Scan Redis and calculate end-to-end latency statistics (created_at → persisted_at)
# Default: scan up to 1000 records
python3 infra/scripts/redis_latency_stats.py

# Scan all records (limit=0)
python3 infra/scripts/redis_latency_stats.py --limit 0

# Scan specific number of records
python3 infra/scripts/redis_latency_stats.py --limit 500
```

**Output includes:**
- `avg_latency_ms`: Average end-to-end latency
- `min_latency_ms`: Minimum latency observed
- `max_latency_ms`: Maximum latency observed
- `p95_latency_ms`: 95th percentile latency
- `p99_latency_ms`: 99th percentile latency

#### Redis Idempotency Key Structure
Each idempotency key uses the format `idempotency:{event_id}` and contains:
```json
{
  "request_hash": "hash_of_request_body",
  "phase": "PHI_COMMITTED",
  "phi_id": "550e8400-e29b-41d4-a716-446655440000",
  "version": 5,
  "created_at": "2024-12-20T15:30:45.123456Z",
  "updated_at": "2024-12-20T15:30:46.567890Z",
  "persisted_at": "2024-12-20T15:30:47.234567Z"
}
```

**Timestamp Fields:**
- `created_at`: When the API initially admits the request (idempotency key creation)
- `updated_at`: When the request phase last changed (PHI validation, commitment, etc.)
- `persisted_at`(optional): When the Flink job confirms De-ID persistence in PostgreSQL

**Latency Measurements:**
- API processing: `updated_at - created_at`
- De-ID confirmation: `persisted_at - updated_at`
- Total request lifecycle: `persisted_at - created_at`


## REST API Services (Quick Copy / Paste)

### Create Patient
```bash
curl -X POST http://localhost:8080/api/v1/patient \
  -H "Content-Type: application/json" \
  -d '{
    "event_id": "550e8400-e29b-41d4-a716-446655440000",
    "name": "Jane Smith",
    "dob": "1985-03-20",
    "favorite_color": "red"
  }'
```
Expected:
202 { event_id, phi_id, phase="PHI_COMMITTED", version }

---

### Update Patient
```bash
curl -X PUT http://localhost:8080/api/v1/patient/${phi_id} \
  -H "Content-Type: application/json" \
  -d '{
    "event_id": "550e8400-e29b-41d4-a716-446655440000",
    "name": "Jane Smith",
    "dob": "1985-03-20",
    "favorite_color": "purple"
  }'
```
Expected:
202 { event_id, phi_id, phase="PHI_COMMITTED", version }

---

### Get De‑Identified Record (by phi_id)
```bash
curl http://localhost:8080/api/v1/deid/patients/by-deid/${de_id}
```
---

### Get PHI Record (Privileged)
```bash
curl http://localhost:8080/api/v1/phi/patients/by-phi_id/${phi_id}
curl http://localhost:8080/api/v1/deid/patients/by-phi_id/${phi_id} (with the explanation that it reads de_id + persisted_version from phi_patient_head)
curl http://localhost:8080/api/v1/phi/patients/by-deid/${de_id}
curl http://localhost:8080/api/v1/phi/patients/by-phi_id/${phi_id} (and version param)
```