"""
FastAPI Application — Criminal Intelligence Network Analysis Platform.

Endpoints:
  POST /api/v1/normalize               — Normalize a person document
  POST /api/v1/resolve                 — Resolve entity to master person
  GET  /api/v1/master/{master_id}      — Get master person profile
  GET  /api/v1/master/{master_id}/network — Expand ego-network
  GET  /api/v1/persons/connected/{id}  — Get connected persons
  GET  /api/v1/relationships           — Query relationships
  POST /api/v1/graph/traverse          — Parameterized graph traversal
  POST /api/v1/graph/path              — Shortest path between two persons
  GET  /api/v1/risk/{master_id}        — Risk score
  GET  /api/v1/intelligence/hotspots   — Geographic hotspot analysis
  POST /api/v1/search/persons          — Multi-field person search
"""
from __future__ import annotations

import time
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Query, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from src.api.models import (
    GraphTraversalRequest,
    HotspotResponse,
    MasterPersonResponse,
    NetworkResponse,
    NormalizeRequest,
    NormalizeResponse,
    PathResponse,
    PersonSearchRequest,
    RelationshipResponse,
    ResolveRequest,
    ResolveResponse,
    RiskResponse,
)
from src.core.config import get_settings
from src.core.elasticsearch import close_es_client, get_es_client
from src.core.kafka import stop_producer
from src.core.logging import configure_logging, get_logger
from src.entity_resolution.pipeline import EntityResolutionPipeline
from src.graph_intelligence.network_analyzer import NetworkAnalyzer
from src.graph_intelligence.risk_scorer import RiskScorer
from src.normalization.pipeline import NormalizationPipeline
from src.relationships.generator import RelationshipGenerator

logger = get_logger(__name__)
settings = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    configure_logging()
    logger.info("api_starting", environment=settings.environment)
    yield
    await stop_producer()
    await close_es_client()
    logger.info("api_shutdown")


app = FastAPI(
    title="Criminal Intelligence Network Analysis Platform",
    description=(
        "Enterprise-scale entity resolution, relationship generation, "
        "and graph intelligence API for police criminal records."
    ),
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Middleware ─────────────────────────────────────────────────────────────────

@app.middleware("http")
async def log_requests(request: Request, call_next):
    start = time.perf_counter()
    response = await call_next(request)
    duration_ms = round((time.perf_counter() - start) * 1000, 2)
    logger.info(
        "http_request",
        method=request.method,
        path=request.url.path,
        status=response.status_code,
        duration_ms=duration_ms,
    )
    return response


# ── Health ─────────────────────────────────────────────────────────────────────

@app.get("/health", tags=["System"])
async def health():
    es = get_es_client()
    try:
        info = await es.info()
        es_status = "ok"
    except Exception as e:
        es_status = f"error: {e}"
    return {
        "status": "ok",
        "elasticsearch": es_status,
        "version": "1.0.0",
    }


# ── Normalization Endpoint ─────────────────────────────────────────────────────

@app.post(
    "/api/v1/normalize",
    response_model=NormalizeResponse,
    tags=["Normalization"],
    summary="Normalize a raw person document",
)
async def normalize_person(request: NormalizeRequest):
    """
    Normalize a raw police record from any source index.
    Extracts and cleans name, phone, address, FIR fields.
    Generates blocking keys for entity resolution.
    Does NOT modify source index.
    """
    pipeline = NormalizationPipeline()
    try:
        normalized = pipeline.process(
            raw_doc=request.raw_doc,
            source_index=request.source_index,
            source_id=request.source_id,
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

    # Index to normalized_person
    es = get_es_client()
    doc_id = normalized.pop("_id")
    await es.index(
        index=settings.index_normalized_person,
        id=doc_id,
        document=normalized,
    )
    normalized["normalized_id"] = doc_id

    return NormalizeResponse(
        normalized_id=doc_id,
        normalized_name=normalized.get("normalized_name", ""),
        phonetic_name=normalized.get("phonetic_name", ""),
        blocking_keys=normalized.get("blocking_keys", []),
        normalized_phones=normalized.get("normalized_phones", []),
        district=normalized.get("district", ""),
        processing_status=normalized.get("processing_status", ""),
    )


# ── Entity Resolution Endpoint ─────────────────────────────────────────────────

@app.post(
    "/api/v1/resolve",
    response_model=ResolveResponse,
    tags=["Entity Resolution"],
    summary="Resolve a normalized person to a master identity",
)
async def resolve_entity(request: ResolveRequest):
    """
    Run entity resolution for a normalized_person document.
    Compares against existing master persons using blocking + scoring.
    Creates or merges master_person record.
    """
    es = get_es_client()
    try:
        resp = await es.get(
            index=settings.index_normalized_person,
            id=request.normalized_id,
        )
        normalized_doc = resp["_source"]
        normalized_doc["normalized_id"] = request.normalized_id
    except Exception:
        raise HTTPException(
            status_code=404,
            detail=f"Normalized document not found: {request.normalized_id}",
        )

    er_pipeline = EntityResolutionPipeline(es)
    result = await er_pipeline.resolve(normalized_doc)

    return ResolveResponse(
        action=result["action"],
        master_person_id=result.get("master_person_id"),
        score=result.get("score", 0.0),
        normalized_id=request.normalized_id,
    )


# ── Master Person Endpoints ────────────────────────────────────────────────────

@app.get(
    "/api/v1/master/{master_id}",
    response_model=MasterPersonResponse,
    tags=["Master Person"],
    summary="Get a master person profile",
)
async def get_master_person(master_id: str):
    """Return full master person profile with all merged attributes."""
    es = get_es_client()
    try:
        resp = await es.get(index=settings.index_master_person, id=master_id)
        doc = resp["_source"]
    except Exception:
        raise HTTPException(status_code=404, detail=f"Master person not found: {master_id}")

    return MasterPersonResponse(**doc)


@app.get(
    "/api/v1/master/{master_id}/network",
    response_model=NetworkResponse,
    tags=["Graph Intelligence"],
    summary="Expand the relationship network around a master person",
)
async def get_network(
    master_id: str,
    depth: int = Query(default=2, ge=1, le=4, description="BFS depth"),
    min_strength: float = Query(default=0.0, ge=0.0, le=1.0),
    rel_types: Optional[str] = Query(default=None, description="Comma-separated relationship types"),
):
    """
    BFS network expansion. Returns nodes and edges up to `depth` hops
    from the specified master person.
    """
    es = get_es_client()
    analyzer = NetworkAnalyzer(es)
    rel_list = [r.strip() for r in rel_types.split(",")] if rel_types else None

    result = await analyzer.expand_network(
        master_id=master_id,
        max_depth=depth,
        rel_types=rel_list,
        min_strength=min_strength,
    )
    return NetworkResponse(**result)


@app.get(
    "/api/v1/persons/connected/{master_id}",
    tags=["Graph Intelligence"],
    summary="Get persons directly connected to a master person",
)
async def get_connected_persons(
    master_id: str,
    rel_type: Optional[str] = None,
    min_strength: float = Query(default=0.3, ge=0.0, le=1.0),
    size: int = Query(default=20, ge=1, le=100),
):
    """Return list of persons with direct relationships to the given master person."""
    es = get_es_client()
    must: List[Dict] = [
        {
            "bool": {
                "should": [
                    {"term": {"source_master_id": master_id}},
                    {"term": {"target_master_id": master_id}},
                ],
                "minimum_should_match": 1,
            }
        },
        {"term": {"is_active": True}},
        {"range": {"strength": {"gte": min_strength}}},
    ]
    if rel_type:
        must.append({"term": {"relationship_type": rel_type}})

    resp = await es.search(
        index=settings.index_relationships,
        query={"bool": {"must": must}},
        size=size,
        sort=[{"strength": "desc"}],
    )
    return {"relationships": [h["_source"] for h in resp["hits"]["hits"]]}


# ── Relationship Endpoints ─────────────────────────────────────────────────────

@app.get(
    "/api/v1/relationships",
    tags=["Relationships"],
    summary="Query relationships by type, FIR, or district",
)
async def get_relationships(
    relationship_type: Optional[str] = None,
    fir_no: Optional[str] = None,
    district: Optional[str] = None,
    min_strength: float = Query(default=0.0, ge=0.0, le=1.0),
    from_: int = Query(default=0, alias="from", ge=0),
    size: int = Query(default=20, ge=1, le=100),
):
    es = get_es_client()
    must: List[Dict] = []
    if relationship_type:
        must.append({"term": {"relationship_type": relationship_type}})
    if fir_no:
        must.append({"term": {"fir_numbers": fir_no}})
    if district:
        must.append({"term": {"districts": district}})
    if min_strength > 0:
        must.append({"range": {"strength": {"gte": min_strength}}})

    query = {"bool": {"must": must}} if must else {"match_all": {}}
    resp = await es.search(
        index=settings.index_relationships,
        query=query,
        size=size,
        from_=from_,
        sort=[{"strength": "desc"}, {"last_seen": "desc"}],
    )
    return {
        "total": resp["hits"]["total"]["value"],
        "relationships": [h["_source"] for h in resp["hits"]["hits"]],
    }


# ── Graph Traversal Endpoints ──────────────────────────────────────────────────

@app.post(
    "/api/v1/graph/traverse",
    response_model=NetworkResponse,
    tags=["Graph Intelligence"],
    summary="Parameterized graph traversal",
)
async def graph_traverse(request: GraphTraversalRequest):
    """Full parameterized BFS traversal with relationship type and strength filters."""
    es = get_es_client()
    analyzer = NetworkAnalyzer(es)
    result = await analyzer.expand_network(
        master_id=request.master_id,
        max_depth=request.max_depth,
        rel_types=request.rel_types,
        min_strength=request.min_strength,
    )
    return NetworkResponse(**result)


@app.post(
    "/api/v1/graph/path",
    response_model=PathResponse,
    tags=["Graph Intelligence"],
    summary="Find shortest relationship path between two persons",
)
async def find_path(source_id: str, target_id: str, max_hops: int = Query(default=4, ge=1, le=6)):
    """Find the shortest relationship chain between two master persons."""
    es = get_es_client()
    analyzer = NetworkAnalyzer(es)
    path = await analyzer.find_path(source_id, target_id, max_hops)
    return PathResponse(
        source_id=source_id,
        target_id=target_id,
        path=path,
        hop_count=len(path) - 1 if path else None,
        found=path is not None,
    )


# ── Risk Endpoint ──────────────────────────────────────────────────────────────

@app.get(
    "/api/v1/risk/{master_id}",
    response_model=RiskResponse,
    tags=["Risk Intelligence"],
    summary="Compute criminal risk score for a master person",
)
async def get_risk_score(master_id: str):
    """Compute and return 0–10 risk score with factor breakdown."""
    es = get_es_client()
    scorer = RiskScorer(es)
    score, factors = await scorer.compute(master_id)
    return RiskResponse(
        master_person_id=master_id,
        risk_score=score,
        risk_factors=factors,
    )


# ── Intelligence Endpoints ─────────────────────────────────────────────────────

@app.get(
    "/api/v1/intelligence/hotspots",
    tags=["Intelligence"],
    summary="Geographic crime hotspot analysis",
)
async def get_hotspots(
    top_n: int = Query(default=10, ge=1, le=50),
    district: Optional[str] = None,
):
    """
    Identify top crime hotspots by aggregating relationship density
    and FIR counts per district / police station.
    """
    es = get_es_client()
    filter_clause: List[Dict] = []
    if district:
        filter_clause.append({"term": {"districts": district}})

    resp = await es.search(
        index=settings.index_relationships,
        query={"bool": {"filter": filter_clause}} if filter_clause else {"match_all": {}},
        aggs={
            "by_district": {
                "terms": {"field": "districts", "size": top_n},
                "aggs": {
                    "by_rel_type": {
                        "terms": {"field": "relationship_type", "size": 5}
                    },
                    "total_strength": {
                        "sum": {"field": "strength"}
                    },
                },
            }
        },
        size=0,
    )

    hotspots = []
    for bucket in resp["aggregations"]["by_district"]["buckets"]:
        rel_types = [
            {"type": b["key"], "count": b["doc_count"]}
            for b in bucket["by_rel_type"]["buckets"]
        ]
        hotspots.append({
            "district": bucket["key"],
            "relationship_count": bucket["doc_count"],
            "total_strength": round(bucket["total_strength"]["value"], 2),
            "top_relationship_types": rel_types,
        })

    return {"total": len(hotspots), "hotspots": hotspots}


@app.get(
    "/api/v1/intelligence/gangs",
    tags=["Intelligence"],
    summary="Gang / community detection results",
)
async def get_gangs(
    district: Optional[str] = None,
    min_risk: float = Query(default=5.0, ge=0.0, le=10.0),
    size: int = Query(default=50, ge=1, le=200),
):
    """Return persons grouped by gang_ids (populated by community detection batch job)."""
    es = get_es_client()
    must: List[Dict] = [{"range": {"risk_score": {"gte": min_risk}}}]
    if district:
        must.append({"term": {"districts": district}})

    resp = await es.search(
        index=settings.index_master_person,
        query={"bool": {"must": must}},
        aggs={"by_gang": {"terms": {"field": "gang_ids", "size": 50}}},
        size=size,
        sort=[{"risk_score": "desc"}],
        _source=["master_person_id", "primary_name", "risk_score", "gang_ids", "districts"],
    )
    return {
        "total": resp["hits"]["total"]["value"],
        "persons": [h["_source"] for h in resp["hits"]["hits"]],
        "gang_clusters": resp["aggregations"]["by_gang"]["buckets"],
    }


# ── Person Search ──────────────────────────────────────────────────────────────

@app.post(
    "/api/v1/search/persons",
    tags=["Search"],
    summary="Multi-field person search",
)
async def search_persons(request: PersonSearchRequest):
    """
    Search master persons by name (fuzzy + phonetic), phone (exact),
    FIR number, district, or role.
    """
    es = get_es_client()
    must: List[Dict] = []
    should: List[Dict] = []

    if request.name:
        should += [
            {"match": {"primary_name": {"query": request.name, "fuzziness": "AUTO", "boost": 2}}},
            {"match": {"primary_name.phonetic": {"query": request.name, "boost": 3}}},
            {"match": {"aliases": {"query": request.name, "fuzziness": "AUTO"}}},
        ]

    if request.phone:
        from src.normalization.phone_normalizer import normalize_phone
        norm_phone = normalize_phone(request.phone)
        if norm_phone:
            must.append({"term": {"all_phones": norm_phone}})

    if request.fir_no:
        must.append({"term": {"connected_firs": request.fir_no}})

    if request.district:
        must.append({"term": {"districts": request.district}})

    if request.role:
        must.append({
            "nested": {
                "path": "person_roles",
                "query": {"term": {"person_roles.role": request.role}},
            }
        })

    query: Dict[str, Any] = {
        "bool": {
            "must": must,
            "should": should,
            "minimum_should_match": 1 if should and not must else 0,
        }
    }
    if not must and not should:
        query = {"match_all": {}}

    resp = await es.search(
        index=settings.index_master_person,
        query=query,
        size=request.size,
        from_=request.from_,
        sort=[{"_score": "desc"}, {"risk_score": "desc"}],
    )
    return {
        "total": resp["hits"]["total"]["value"],
        "persons": [h["_source"] for h in resp["hits"]["hits"]],
    }
