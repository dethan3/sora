from pathlib import Path

import pytest
import yaml

from src.sora.config import PROJECT_ROOT, load_config


def test_load_config_resolves_database_path_to_project_root(tmp_path, monkeypatch):
    config_file = tmp_path / "custom.yaml"
    config_file.write_text(
        yaml.safe_dump(
            {
                "database_path": "data/sora.db",
                "analysis": {
                    "lookback_days": 90,
                    "short_window": 7,
                    "long_window": 30,
                },
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.chdir(tmp_path)
    config = load_config("custom.yaml")

    assert config.database_path == str((PROJECT_ROOT / "data" / "sora.db").resolve())


def test_load_config_rejects_invalid_analysis_values(tmp_path):
    config_file = tmp_path / "invalid.yaml"
    config_file.write_text(
        yaml.safe_dump(
            {
                "analysis": {
                    "lookback_days": 10,
                    "short_window": 20,
                    "long_window": 5,
                }
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError):
        load_config(str(config_file))
