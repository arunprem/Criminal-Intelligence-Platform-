#!/usr/bin/env python3
"""
Historical Batch Runner CLI.

Usage:
  python scripts/run_historical_batch.py
  python scripts/run_historical_batch.py --indices accused victim
  python scripts/run_historical_batch.py --parallel --batch-size 1000
  python scripts/run_historical_batch.py --reset-checkpoints
"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import click
from rich.console import Console

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.batch.checkpoint import CheckpointManager
from src.batch.historical_processor import HistoricalBatchProcessor
from src.core.config import get_settings
from src.core.elasticsearch import get_es_client
from src.core.logging import configure_logging

console = Console()
settings = get_settings()


async def run(
    indices: tuple[str, ...],
    parallel: bool,
    batch_size: int,
    reset_checkpoints: bool,
) -> None:
    configure_logging()
    es = get_es_client()

    target_indices = list(indices) if indices else settings.raw_indices
    console.print(f"\n[bold cyan]🚀 Historical Batch Processing[/]")
    console.print(f"  Indices  : {target_indices}")
    console.print(f"  Batch    : {batch_size} records")
    console.print(f"  Parallel : {parallel}")
    console.print(f"  Reset    : {reset_checkpoints}\n")

    if reset_checkpoints:
        mgr = CheckpointManager(es)
        for idx in target_indices:
            await mgr.reset(idx)
            console.print(f"  🗑  Checkpoint reset: {idx}")

    processor = HistoricalBatchProcessor()
    stats = await processor.run(
        indices=target_indices,
        batch_size=batch_size,
        parallel=parallel,
    )

    console.print("\n[bold green]✅ Batch Processing Complete[/]")
    for idx, result in stats.items():
        if "error" in result:
            console.print(f"  ❌ {idx}: {result['error']}")
        else:
            console.print(
                f"  ✅ {idx}: processed={result.get('processed', 0):,} "
                f"failed={result.get('failed', 0):,}"
            )

    await es.close()
    await processor.close()


@click.command()
@click.option(
    "--indices", multiple=True,
    help="Source indices to process. Defaults to all raw indices.",
)
@click.option("--parallel/--sequential", default=True, help="Process indices in parallel")
@click.option("--batch-size", default=500, show_default=True, type=int)
@click.option("--reset-checkpoints", is_flag=True, help="Clear checkpoints before starting")
def main(indices, parallel, batch_size, reset_checkpoints):
    """Run historical batch processing for raw source indices."""
    asyncio.run(run(indices, parallel, batch_size, reset_checkpoints))


if __name__ == "__main__":
    main()
