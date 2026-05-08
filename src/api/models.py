"""
Pydantic models for API request/response schemas.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


# ── Request Models ─────────────────────────────────────────────────────────────

class NormalizeRequest(BaseModel):
    source_index: str = Field(..., description="Source index name (e.g. accused)")
    source_id: str = Field(..., description="Document ID in source index")
    raw_doc: Dict[str, Any] = Field(..., description="Raw document from source index")


class ResolveRequest(BaseModel):
    normalized_id: str = Field(..., description="ID from normalized_person index")


class GraphTraversalRequest(BaseModel):
    master_id: str
    max_depth: int = Field(default=2, ge=1, le=4)
    rel_types: Optional[List[str]] = None
    min_strength: float = Field(default=0.0, ge=0.0, le=1.0)


class PersonSearchRequest(BaseModel):
    name: Optional[str] = None
    phone: Optional[str] = None
    fir_no: Optional[str] = None
    district: Optional[str] = None
    role: Optional[str] = None
    from_: int = Field(default=0, alias="from", ge=0)
    size: int = Field(default=20, ge=1, le=100)


# ── Response Models ────────────────────────────────────────────────────────────

class NormalizeResponse(BaseModel):
    normalized_id: str
    normalized_name: str
    phonetic_name: str
    blocking_keys: List[str]
    normalized_phones: List[str]
    district: str
    processing_status: str


class ResolveResponse(BaseModel):
    action: str  # merged | new | review
    master_person_id: Optional[str]
    score: float
    normalized_id: str


class MasterPersonResponse(BaseModel):
    master_person_id: str
    primary_name: str
    name_variants: List[str]
    all_phones: List[str]
    canonical_address: str
    districts: List[str]
    connected_firs: List[str]
    person_roles: List[Dict[str, Any]]
    risk_score: float
    risk_factors: List[Dict[str, Any]]
    aliases: List[str]
    gang_ids: List[str]
    network_size: int
    status: str
    created_at: str
    last_updated: str


class RelationshipResponse(BaseModel):
    relationship_id: str
    source_master_id: str
    target_master_id: str
    relationship_type: str
    strength: float
    fir_numbers: List[str]
    occurrence_count: int
    first_seen: str
    last_seen: str


class NetworkNode(BaseModel):
    master_person_id: str
    primary_name: str
    risk_score: float
    connected_firs_count: int
    districts: List[str]
    is_root: bool


class NetworkEdge(BaseModel):
    source: str
    target: str
    type: str
    strength: float
    fir_numbers: List[str]


class NetworkResponse(BaseModel):
    root_id: str
    depth: int
    node_count: int
    edge_count: int
    nodes: List[NetworkNode]
    edges: List[NetworkEdge]


class RiskResponse(BaseModel):
    master_person_id: str
    risk_score: float
    risk_factors: List[Dict[str, Any]]


class HotspotResponse(BaseModel):
    district: str
    police_station: Optional[str]
    fir_count: int
    accused_count: int
    relationship_density: float
    top_relationship_types: List[Dict[str, Any]]


class PathResponse(BaseModel):
    source_id: str
    target_id: str
    path: Optional[List[str]]
    hop_count: Optional[int]
    found: bool
