"""
Unit tests for integration test sharding.
"""

from pathlib import Path

import pytest

from tests.integration.sharding import (
    FeatureSpec,
    build_shards,
    load_features,
    select_shard,
    write_featureset,
)


def _write_feature(path: Path, scenario_count: int) -> None:
    scenarios = []
    for index in range(scenario_count):
        scenarios.append(
            f"""
  Scenario: scenario {index}
    Given a precondition
"""
        )
    path.write_text(
        "Feature: generated feature\n" + "".join(scenarios), encoding="utf-8"
    )


def _feature_spec(name: str, position: int, scenarios: int) -> FeatureSpec:
    return FeatureSpec(Path(name), position, scenarios)


def test_load_features_ignores_comments_and_expands_scenario_outlines(
    tmp_path: Path,
) -> None:
    regular = tmp_path / "regular.feature"
    regular.write_text(
        """Feature: regular
  Scenario: one
    Given a precondition

  Scenario Outline: outlined <value>
    Given a <value>

    Examples:
      | value |
      | one   |
      | two   |
""",
        encoding="utf-8",
    )
    other = tmp_path / "other.feature"
    _write_feature(other, 1)
    featureset = tmp_path / "featureset"
    featureset.write_text(
        "\n# ignored comment\nregular.feature\nother.feature\n", encoding="utf-8"
    )

    features = load_features(featureset)

    assert [feature.path for feature in features] == [regular, other]
    assert [feature.scenarios for feature in features] == [3, 1]


def test_build_shards_is_deterministic_and_balanced() -> None:
    features = [
        _feature_spec("first.feature", 0, 5),
        _feature_spec("second.feature", 1, 4),
        _feature_spec("third.feature", 2, 3),
        _feature_spec("fourth.feature", 3, 2),
        _feature_spec("fifth.feature", 4, 1),
    ]

    first = build_shards(features, 3)
    second = build_shards(features, 3)

    assert first == second
    assert [[feature.position for feature in shard] for shard in first] == [
        [0],
        [1, 4],
        [2, 3],
    ]
    assert [sum(feature.scenarios for feature in shard) for shard in first] == [
        5,
        5,
        5,
    ]


def test_one_shard_preserves_canonical_order() -> None:
    features = [
        _feature_spec("small.feature", 0, 1),
        _feature_spec("large.feature", 1, 10),
        _feature_spec("medium.feature", 2, 5),
    ]

    shard = select_shard(features, shard_count=1, shard_index=1)

    assert [feature.position for feature in shard] == [0, 1, 2]


@pytest.mark.parametrize(
    "shard_count, shard_index, message",
    [
        (0, 1, "Shard index must be between 1 and 0"),
        (3, 0, "Shard index must be between 1 and 3"),
        (3, 4, "Shard index must be between 1 and 3"),
    ],
)
def test_select_shard_rejects_invalid_index(
    shard_count: int, shard_index: int, message: str
) -> None:
    features = [_feature_spec("feature.feature", 0, 1)]

    with pytest.raises(ValueError, match=message):
        select_shard(features, shard_count, shard_index)


def test_build_shards_rejects_more_shards_than_features() -> None:
    features = [_feature_spec("feature.feature", 0, 1)]

    with pytest.raises(ValueError, match="exceeds feature count"):
        build_shards(features, 2)


def test_load_features_rejects_empty_duplicate_and_missing_lists(
    tmp_path: Path,
) -> None:
    empty = tmp_path / "empty.featureset"
    empty.write_text("\n# comment\n", encoding="utf-8")
    with pytest.raises(ValueError, match="Feature list is empty"):
        load_features(empty)

    feature = tmp_path / "feature.feature"
    _write_feature(feature, 1)
    duplicate = tmp_path / "duplicate.featureset"
    duplicate.write_text("feature.feature\nfeature.feature\n", encoding="utf-8")
    with pytest.raises(ValueError, match="Duplicate feature"):
        load_features(duplicate)

    missing = tmp_path / "missing.featureset"
    missing.write_text("missing.feature\n", encoding="utf-8")
    with pytest.raises(FileNotFoundError, match="does not exist"):
        load_features(missing)


def test_write_featureset_uses_paths_relative_to_output(tmp_path: Path) -> None:
    feature = tmp_path / "features" / "feature.feature"
    feature.parent.mkdir()
    _write_feature(feature, 1)
    output = tmp_path / "staging" / "selected.featureset"

    write_featureset([FeatureSpec(feature, 0, 1)], output)

    assert output.read_text(encoding="utf-8") == "../features/feature.feature\n"


def test_new_feature_is_automatically_assigned_once(tmp_path: Path) -> None:
    feature_names = ["first.feature", "second.feature", "new.feature"]
    for name in feature_names:
        _write_feature(tmp_path / name, 1)
    featureset = tmp_path / "featureset"
    featureset.write_text("\n".join(feature_names) + "\n", encoding="utf-8")

    shards = build_shards(load_features(featureset), 2)
    assigned_paths = [feature.path.name for shard in shards for feature in shard]

    assert sorted(assigned_paths) == sorted(feature_names)
    assert assigned_paths.count("new.feature") == 1


def test_repository_featureset_is_completely_partitioned() -> None:
    featureset = Path(__file__).parents[1] / "integration/ch_backup.featureset"

    features = load_features(featureset)
    shards = build_shards(features, 3)
    assigned_features = [feature for shard in shards for feature in shard]
    shard_weights = [sum(feature.scenarios for feature in shard) for shard in shards]

    assert len(assigned_features) == len(features)
    assert {feature.path for feature in assigned_features} == {
        feature.path for feature in features
    }
    assert max(shard_weights) - min(shard_weights) <= max(
        feature.scenarios for feature in features
    )
