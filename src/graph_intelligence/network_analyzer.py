"""
Network Analyzer — Graph Intelligence Layer.

Performs ego-graph construction and network analytics using
Elasticsearch relationship data. Designed to work without Neo4j
(pure ES-based BFS), with Neo4j as a future upgrade path.

Features:
- BFS network expansion (configurable depth)
- Degree centrality computation
- Community detection (Louvain-style via networkx)
- Connection path finding
- Temporal network slicing
"""
from __future__ import annotations

import asyncio
from collections import deque
from typing import Any, Dict, List, Optional, Set, Tuple

import networkx as nx
from elasticsearch import AsyncElasticsearch

from src.core.config import get_settings
from src.core.logging import get_logger

logger = get_logger(__name__)
settings = get_settings()


class NetworkAnalyzer:
    """
    Graph intelligence engine backed by Elasticsearch relationships index.
    """

    def __init__(self, es: AsyncElasticsearch) -> None:
        self.es = es
        self.rel_index = settings.index_relationships
        self.master_index = settings.index_master_person

    async def expand_network(
        self,
        master_id: str,
        max_depth: int = 2,
        rel_types: Optional[List[str]] = None,
        min_strength: float = 0.0,
    ) -> Dict[str, Any]:
        """
        BFS expansion from a master person up to max_depth hops.
        Returns nodes (persons) and edges (relationships).
        """
        visited: Set[str] = set()
        nodes: Dict[str, Dict[str, Any]] = {}
        edges: List[Dict[str, Any]] = []
        queue: deque = deque([(master_id, 0)])
        visited.add(master_id)

        while queue:
            current_id, depth = queue.popleft()
            if depth >= max_depth:
                continue

            # Fetch direct relationships
            rels = await self._fetch_relationships(
                current_id,
                rel_types=rel_types,
                min_strength=min_strength,
            )

            for rel in rels:
                src = rel["source_master_id"]
                tgt = rel["target_master_id"]
                neighbor_id = tgt if src == current_id else src

                # Add edge
                edges.append({
                    "source": src,
                    "target": tgt,
                    "type": rel["relationship_type"],
                    "strength": rel["strength"],
                    "fir_numbers": rel.get("fir_numbers", []),
                })

                if neighbor_id not in visited:
                    visited.add(neighbor_id)
                    queue.append((neighbor_id, depth + 1))

        # Fetch person metadata for all nodes in parallel
        node_ids = list(visited)
        person_data = await self._fetch_persons_batch(node_ids)
        for p in person_data:
            nodes[p["master_person_id"]] = {
                "master_person_id": p["master_person_id"],
                "primary_name": p.get("primary_name", ""),
                "risk_score": p.get("risk_score", 0.0),
                "connected_firs_count": len(p.get("connected_firs", [])),
                "districts": p.get("districts", []),
                "is_root": p["master_person_id"] == master_id,
            }

        return {
            "root_id": master_id,
            "depth": max_depth,
            "node_count": len(nodes),
            "edge_count": len(edges),
            "nodes": list(nodes.values()),
            "edges": edges,
        }

    async def compute_centrality(
        self,
        master_ids: List[str],
    ) -> Dict[str, float]:
        """
        Compute degree centrality for a set of master persons.
        Returns {master_id: centrality_score}.
        """
        # Build local subgraph
        G = nx.Graph()
        for mid in master_ids:
            rels = await self._fetch_relationships(mid, min_strength=0.3)
            for rel in rels:
                src = rel["source_master_id"]
                tgt = rel["target_master_id"]
                if src in master_ids and tgt in master_ids:
                    G.add_edge(
                        src, tgt,
                        weight=rel.get("strength", 0.5),
                        rel_type=rel["relationship_type"],
                    )

        if not G.nodes:
            return {}

        centrality = nx.degree_centrality(G)
        betweenness = nx.betweenness_centrality(G, weight="weight")

        # Combine: 60% degree + 40% betweenness
        combined: Dict[str, float] = {}
        for node in G.nodes:
            combined[node] = round(
                centrality.get(node, 0) * 0.6
                + betweenness.get(node, 0) * 0.4,
                4,
            )
        return combined

    async def detect_communities(
        self,
        master_ids: List[str],
        min_strength: float = 0.4,
    ) -> Dict[str, Any]:
        """
        Community detection using Louvain algorithm (networkx-community).
        Returns {community_id: [master_ids], master_id: community_id}.
        """
        try:
            import community as community_louvain  # python-louvain
        except ImportError:
            logger.warning("python-louvain not installed; skipping community detection")
            return {"communities": {}, "assignments": {}}

        G = nx.Graph()
        for mid in master_ids:
            rels = await self._fetch_relationships(mid, min_strength=min_strength)
            for rel in rels:
                src = rel["source_master_id"]
                tgt = rel["target_master_id"]
                if src in master_ids and tgt in master_ids:
                    existing = G.get_edge_data(src, tgt)
                    if existing:
                        G[src][tgt]["weight"] = max(
                            existing["weight"], rel.get("strength", 0.5)
                        )
                    else:
                        G.add_edge(src, tgt, weight=rel.get("strength", 0.5))

        if not G.nodes:
            return {"communities": {}, "assignments": {}}

        partition = community_louvain.best_partition(G, weight="weight")
        communities: Dict[int, List[str]] = {}
        for node, comm_id in partition.items():
            communities.setdefault(comm_id, []).append(node)

        return {
            "total_communities": len(communities),
            "communities": {str(k): v for k, v in communities.items()},
            "assignments": partition,
        }

    async def find_path(
        self,
        source_id: str,
        target_id: str,
        max_hops: int = 4,
    ) -> Optional[List[str]]:
        """
        Find shortest relationship path between two master persons.
        Returns list of master_ids from source to target, or None if no path.
        """
        # BFS up to max_hops
        queue: deque = deque([(source_id, [source_id])])
        visited: Set[str] = {source_id}

        while queue:
            current, path = queue.popleft()
            if len(path) > max_hops + 1:
                break

            rels = await self._fetch_relationships(current)
            for rel in rels:
                src = rel["source_master_id"]
                tgt = rel["target_master_id"]
                neighbor = tgt if src == current else src

                if neighbor == target_id:
                    return path + [neighbor]

                if neighbor not in visited:
                    visited.add(neighbor)
                    queue.append((neighbor, path + [neighbor]))

        return None

    # ── Private ───────────────────────────────────────────────────────────────

    async def _fetch_relationships(
        self,
        master_id: str,
        rel_types: Optional[List[str]] = None,
        min_strength: float = 0.0,
        size: int = 200,
    ) -> List[Dict[str, Any]]:
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
        ]
        if min_strength > 0:
            must.append({"range": {"strength": {"gte": min_strength}}})
        if rel_types:
            must.append({"terms": {"relationship_type": rel_types}})

        resp = await self.es.search(
            index=self.rel_index,
            query={"bool": {"must": must}},
            size=size,
            _source=True,
        )
        return [h["_source"] for h in resp["hits"]["hits"]]

    async def _fetch_persons_batch(
        self, master_ids: List[str]
    ) -> List[Dict[str, Any]]:
        if not master_ids:
            return []
        resp = await self.es.search(
            index=self.master_index,
            query={"ids": {"values": master_ids}},
            size=len(master_ids),
        )
        return [h["_source"] for h in resp["hits"]["hits"]]
