#!/usr/bin/env python3
"""
Bootstrap Script — Creates all intelligence layer Elasticsearch indices.

Run once before starting any pipeline:
  python scripts/bootstrap_indices.py

Options:
  --reset     Delete and recreate all indices (WARNING: destructive)
  --dry-run   Print mappings without creating
"""
from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

import click
from rich.console import Console
from rich.table import Table

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.core.config import get_settings
from src.core.elasticsearch import ensure_index, get_es_client
from src.core.logging import configure_logging

console = Console()
settings = get_settings()

MAPPINGS_DIR = Path(__file__).parent.parent / "es_mappings"

INDICES = [
    (settings.index_normalized_person, "normalized_person.json"),
    (settings.index_master_person, "master_person.json"),
    (settings.index_relationships, "relationships.json"),
    (settings.index_relationship_events, "relationship_events.json"),
    ("pipeline_checkpoints", None),  # simple index, no custom mapping
]

CHECKPOINT_MAPPING = {
    "settings": {"number_of_shards": 1, "number_of_replicas": 1},
    "mappings": {
        "properties": {
            "index": {"type": "keyword"},
            "last_processed_id": {"type": "keyword"},
            "count": {"type": "long"},
            "status": {"type": "keyword"},
            "started_at": {"type": "date"},
            "updated_at": {"type": "date"},
            "completed_at": {"type": "date"},
        }
    },
}


async def bootstrap(reset: bool = False, dry_run: bool = False) -> None:
    configure_logging()
    es = get_es_client()

    # Verify cluster health
    console.print("\n[bold cyan]🔍 Checking Elasticsearch cluster health...[/]")
    try:
        health = await es.cluster.health(wait_for_status="yellow", timeout="30s")
        console.print(f"  ✅ Cluster status: [green]{health['status']}[/]")
        console.print(f"  📦 Nodes: {health['number_of_nodes']}")
    except Exception as exc:
        console.print(f"  ❌ Cannot reach Elasticsearch: {exc}")
        sys.exit(1)

    table = Table(title="Index Bootstrap Results", show_lines=True)
    table.add_column("Index", style="cyan")
    table.add_column("Action", style="yellow")
    table.add_column("Status", style="green")

    for index_name, mapping_file in INDICES:
        if mapping_file:
            mapping_path = MAPPINGS_DIR / mapping_file
            with open(mapping_path) as f:
                mapping = json.load(f)
        else:
            mapping = CHECKPOINT_MAPPING

        if dry_run:
            table.add_row(index_name, "DRY_RUN", "would create")
            continue

        action = "created"
        try:
            if reset:
                exists = await es.indices.exists(index=index_name)
                if exists:
                    await es.indices.delete(index=index_name)
                    action = "deleted+recreated"
            created = await ensure_index(es, index_name, mapping)
            status = "✅ created" if created else "⏭ already exists"
        except Exception as exc:
            status = f"❌ {exc}"
            action = "error"

        table.add_row(index_name, action, status)

    console.print(table)

    if not dry_run:
        console.print("\n[bold green]✅ Bootstrap complete![/]")
        console.print("  Next steps:")
        console.print("  1. Run historical batch: python scripts/run_historical_batch.py")
        console.print("  2. Start API:           uvicorn src.api.main:app --reload")
        console.print("  3. Start workers:       python -m src.workers.normalization_worker")

    await es.close()


@click.command()
@click.option("--reset", is_flag=True, help="Delete and recreate all indices")
@click.option("--dry-run", is_flag=True, help="Show what would be done without executing")
def main(reset: bool, dry_run: bool) -> None:
    """Bootstrap Elasticsearch indices for the CIN platform."""
    if reset:
        console.print("[bold red]⚠️  WARNING: --reset will DELETE all intelligence indices![/]")
        if not click.confirm("Are you sure?"):
            console.print("Aborted.")
            return
    asyncio.run(bootstrap(reset=reset, dry_run=dry_run))


if __name__ == "__main__":
    main()
