# Criminal Intelligence Network Analysis Platform

> **Enterprise-scale entity resolution, criminal network analysis, and graph intelligence platform for police investigative systems.**

---

## Architecture

```
Raw Indices (accused, victim, complainant, witness)  ← IMMUTABLE
        ↓
normalized_person     ← Cleaned, phonetic, blocking keys, vectors
        ↓
master_person         ← Unified identity via entity resolution
        ↓
relationships         ← Auto-generated from FIR + shared attributes
        ↓
relationship_events   ← Immutable audit log
        ↓
Graph Intelligence    ← Risk scoring, centrality, communities, hotspots
        ↓
Agentic AI (future)  ← LangGraph + Neo4j + LLM investigation agents
```

---

## Quick Start

### 1. Prerequisites

- Python 3.11+
- Docker & Docker Compose
- Elasticsearch 8.x (with Phonetic Analysis plugin)

### 2. Install

```bash
cp .env.example .env        # Configure your ES/Kafka/Redis endpoints
pip install -e ".[dev]"
```

### 3. Start Infrastructure (Development)

```bash
docker compose up -d es01 es02 es03 kafka redis
```

### 4. Bootstrap Elasticsearch Indices

```bash
python scripts/bootstrap_indices.py
```

### 5. Run Historical Batch (first-time data load)

```bash
# Disable replicas and refresh for fast initial load
# Then run:
python scripts/run_historical_batch.py --parallel --batch-size 500
```

### 6. Start API

```bash
uvicorn src.api.main:app --reload --host 0.0.0.0 --port 8000
```

### 7. Start Kafka Workers

```bash
# Terminal 1
python -m src.workers.normalization_worker

# Terminal 2
python -m src.workers.resolution_worker

# Terminal 3
python -m src.workers.relationship_worker
```

---

## Key Indices

| Index | Purpose | Shards |
|---|---|---|
| `normalized_person` | Cleaned + phonetic + blocking | 10 |
| `master_person` | Unified identity profiles | 5 |
| `relationships` | All relationship types | 5 |
| `relationship_events` | Immutable audit log | 3 |

---

## API Endpoints

| Method | Endpoint | Description |
|---|---|---|
| GET | `/health` | Cluster health |
| POST | `/api/v1/normalize` | Normalize a person document |
| POST | `/api/v1/resolve` | Resolve to master identity |
| GET | `/api/v1/master/{id}` | Get master person profile |
| GET | `/api/v1/master/{id}/network` | BFS network expansion |
| GET | `/api/v1/persons/connected/{id}` | Direct connections |
| GET | `/api/v1/relationships` | Query relationships |
| POST | `/api/v1/graph/traverse` | Parameterized traversal |
| POST | `/api/v1/graph/path` | Shortest path between persons |
| GET | `/api/v1/risk/{id}` | Criminal risk score (0–10) |
| GET | `/api/v1/intelligence/hotspots` | Geographic hotspot analysis |
| GET | `/api/v1/intelligence/gangs` | Gang community detection |
| POST | `/api/v1/search/persons` | Multi-field person search |

Full Swagger docs: `http://localhost:8000/docs`

---

## Entity Resolution Strategy

```
1. Blocking   → Multi-family keys (phone prefix, district+name, phonetic...)
2. Retrieval  → ES fuzzy + phonetic + kNN vector search (top-100 candidates)
3. Scoring    → Weighted signals (phone 40%, DOB 10%, relative name 10%...)
4. Decision   → ≥0.75 auto-merge | 0.55-0.74 review queue | <0.55 new master
```

**Thresholds configurable via `.env`:**
```
ER_AUTO_MERGE_THRESHOLD=0.75
ER_REVIEW_THRESHOLD=0.55
```

---

## Relationship Types

| Type | Generated From |
|---|---|
| `ACCUSED_IN` | Person in accused index for FIR |
| `VICTIM_IN` | Person in victim index for FIR |
| `WITNESS_IN` | Person in witness index for FIR |
| `COMPLAINANT_IN` | Person in complainant index for FIR |
| `CO_ACCUSED_WITH` | Two accused in same FIR |
| `SHARES_PHONE` | Same phone number in master profiles |
| `SHARES_ADDRESS` | Same address locality |
| `RELATED_TO` | Shared relative name |
| `ASSOCIATED_WITH` | Accused linked to victim in same FIR |

---

## Running Tests

```bash
pytest tests/ -v --cov=src --cov-report=term-missing
```

---

## Production Deployment

```bash
# Kubernetes
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/configmaps/
kubectl apply -f k8s/api-deployment.yaml
kubectl apply -f k8s/worker-deployment.yaml
```

---

## Future Roadmap

- **Neo4j Integration** — Multi-hop graph traversal (shadow-write architecture)
- **Face Embedding** — 512-dim vector field in master_person for biometric matching
- **LangGraph Agents** — Natural language investigation queries
- **LLM Reports** — AI-generated criminal network summary reports
- **Real-time Alerting** — Kafka stream → alert when known person appears in new FIR

---

## Design Principles

1. ✅ Raw indices are **never modified**
2. ✅ Relationships are **incremental** (additive only)
3. ✅ Every merge has **full audit trail** in `merge_history`
4. ✅ Every relationship has **evidence sources** traceable to raw docs
5. ✅ Graph rebuilds are **never full** — always incremental updates
