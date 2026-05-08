"""
Core application settings loaded from environment variables.
Uses pydantic-settings for typed, validated configuration.
"""
from __future__ import annotations

from functools import lru_cache
from typing import List

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ── Elasticsearch ─────────────────────────────────────────────────────────
    es_hosts: str = "http://localhost:9200"
    es_username: str = "elastic"
    es_password: str = "changeme"
    es_ca_cert: str = ""
    es_request_timeout: int = 60
    es_max_retries: int = 3

    # ── Remote Source Elasticsearch (Optional) ────────────────────────────────
    remote_es_url: Optional[str] = None
    remote_es_username: Optional[str] = None
    remote_es_password: Optional[str] = None

    @property
    def es_hosts_list(self) -> List[str]:
        return [h.strip() for h in self.es_hosts.split(",")]

    # ── Raw Source Indices (IMMUTABLE) ────────────────────────────────────────
    raw_index_accused: str = "accused"
    raw_index_victim: str = "victim"
    raw_index_complainant: str = "complainant"
    raw_index_witness: str = "witness"

    @property
    def raw_indices(self) -> List[str]:
        return ["person_data"]

    # ── Intelligence Indices ──────────────────────────────────────────────────
    index_normalized_person: str = "normalized_person"
    index_master_person: str = "master_person"
    index_relationships: str = "relationships"
    index_relationship_events: str = "relationship_events"
    index_checkpoint: str = "pipeline_checkpoints"

    # ── Kafka ─────────────────────────────────────────────────────────────────
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic_person_raw: str = "person_raw"
    kafka_topic_person_normalized: str = "person_normalized"
    kafka_topic_person_resolved: str = "person_resolved"
    kafka_topic_relationships_generated: str = "relationships_generated"
    kafka_topic_review_queue: str = "review_queue"
    kafka_consumer_group_normalization: str = "normalization-workers"
    kafka_consumer_group_resolution: str = "resolution-workers"
    kafka_consumer_group_relationship: str = "relationship-workers"
    kafka_consumer_group_graph: str = "graph-workers"

    # ── Redis ─────────────────────────────────────────────────────────────────
    redis_url: str = "redis://localhost:6379/0"

    # ── FastAPI ───────────────────────────────────────────────────────────────
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    api_workers: int = 4
    api_debug: bool = False
    api_secret_key: str = "change-me-in-production"

    # ── Entity Resolution ─────────────────────────────────────────────────────
    er_auto_merge_threshold: float = 0.75
    er_review_threshold: float = 0.55
    er_max_candidates: int = 100
    er_batch_size: int = 500

    # ── Batch Processing ──────────────────────────────────────────────────────
    batch_size: int = 500
    batch_checkpoint_every: int = 10000
    batch_workers: int = 4
    batch_max_retries: int = 5

    # ── Embedding ─────────────────────────────────────────────────────────────
    embedding_model: str = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
    embedding_enabled: bool = False
    embedding_dimensions: int = 384

    # ── Observability ─────────────────────────────────────────────────────────
    log_level: str = "INFO"
    prometheus_enabled: bool = True
    environment: str = "development"

    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        valid = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        upper = v.upper()
        if upper not in valid:
            raise ValueError(f"log_level must be one of {valid}")
        return upper


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return cached settings singleton."""
    return Settings()
