"""
Build deterministic, balanced shards from a Behave feature list.
"""

import argparse
import os
from dataclasses import dataclass
from pathlib import Path
from typing import List, Sequence

from behave.parser import parse_file


@dataclass(frozen=True)
class FeatureSpec:
    """A feature file and its weight in expanded Behave scenarios."""

    path: Path
    position: int
    scenarios: int


def load_features(featureset: Path) -> List[FeatureSpec]:
    """Load, validate, and weigh features from a Behave feature list."""
    if not featureset.is_file():
        raise FileNotFoundError(f"Feature list does not exist: {featureset}")

    features: List[FeatureSpec] = []
    seen = set()
    featureset_dir = featureset.resolve().parent

    for line_number, raw_line in enumerate(
        featureset.read_text(encoding="utf-8").splitlines(), start=1
    ):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue

        path = Path(line)
        if not path.is_absolute():
            path = featureset_dir / path
        path = path.resolve()

        if path in seen:
            raise ValueError(
                f"Duplicate feature at {featureset}:{line_number}: {raw_line}"
            )
        if not path.is_file():
            raise FileNotFoundError(
                f"Feature at {featureset}:{line_number} does not exist: {path}"
            )

        seen.add(path)
        feature = parse_file(str(path))
        features.append(
            FeatureSpec(
                path=path,
                position=len(features),
                scenarios=len(list(feature.walk_scenarios())),
            )
        )

    if not features:
        raise ValueError(f"Feature list is empty: {featureset}")

    return features


def build_shards(
    features: Sequence[FeatureSpec], shard_count: int
) -> List[List[FeatureSpec]]:
    """Distribute whole features using deterministic LPT scheduling."""
    if shard_count < 1:
        raise ValueError("Shard count must be greater than zero")
    if shard_count > len(features):
        raise ValueError(
            f"Shard count ({shard_count}) exceeds feature count ({len(features)})"
        )

    shards: List[List[FeatureSpec]] = [[] for _ in range(shard_count)]
    shard_weights = [0] * shard_count

    for feature in sorted(features, key=lambda item: (-item.scenarios, item.position)):
        shard_index = min(
            range(shard_count), key=lambda index: (shard_weights[index], index)
        )
        shards[shard_index].append(feature)
        shard_weights[shard_index] += feature.scenarios

    for shard in shards:
        shard.sort(key=lambda item: item.position)

    return shards


def select_shard(
    features: Sequence[FeatureSpec], shard_count: int, shard_index: int
) -> List[FeatureSpec]:
    """Return one shard using a one-based shard index."""
    if not 1 <= shard_index <= shard_count:
        raise ValueError(
            f"Shard index must be between 1 and {shard_count}, got {shard_index}"
        )
    return build_shards(features, shard_count)[shard_index - 1]


def write_featureset(features: Sequence[FeatureSpec], output: Path) -> None:
    """Write feature paths relative to the generated list location."""
    output.parent.mkdir(parents=True, exist_ok=True)
    output_dir = output.resolve().parent
    lines = [os.path.relpath(feature.path, output_dir) for feature in features]
    output.write_text("\n".join(lines) + "\n", encoding="utf-8")


def print_distribution(shards: Sequence[Sequence[FeatureSpec]]) -> None:
    """Print the deterministic assignment for CI diagnostics."""
    for index, shard in enumerate(shards, start=1):
        scenarios = sum(feature.scenarios for feature in shard)
        print(
            f"Shard {index}/{len(shards)}: {len(shard)} features, {scenarios} scenarios"
        )
        for feature in shard:
            print(f"  {feature.scenarios:3d}  {feature.path.name}")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a deterministic shard from a Behave feature list"
    )
    parser.add_argument("--featureset", required=True, type=Path)
    parser.add_argument("--shards", required=True, type=int)
    parser.add_argument("--index", required=True, type=int)
    parser.add_argument("--output", required=True, type=Path)
    return parser.parse_args()


def cli_main() -> None:
    """CLI entry point."""
    args = _parse_args()
    try:
        features = load_features(args.featureset)
        shards = build_shards(features, args.shards)
        if not 1 <= args.index <= args.shards:
            raise ValueError(
                f"Shard index must be between 1 and {args.shards}, got {args.index}"
            )
        print_distribution(shards)
        write_featureset(shards[args.index - 1], args.output)
        print(f"Selected shard {args.index}/{args.shards}: {args.output}")
    except (FileNotFoundError, ValueError) as error:
        raise SystemExit(str(error)) from error


if __name__ == "__main__":
    cli_main()
