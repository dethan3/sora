from src.sora.domain import AlertDirection, AlertMetric, AlertRule, Asset, AssetType, Market
from src.sora.repository import SQLiteRepository


def test_repository_upserts_and_lists_assets(tmp_path):
    repository = SQLiteRepository(str(tmp_path / "sora.db"))
    repository.initialize()

    repository.upsert_asset(
        Asset(
            code="510300",
            name="沪深300ETF",
            asset_type=AssetType.FUND,
            market=Market.CN,
        )
    )

    assets = repository.list_assets()
    assert len(assets) == 1
    assert assets[0].code == "510300"
    assert assets[0].asset_type == AssetType.FUND


def test_repository_adds_and_lists_alert_rules(tmp_path):
    repository = SQLiteRepository(str(tmp_path / "sora.db"))
    repository.initialize()

    created = repository.add_alert_rule(
        AlertRule(
            asset_code="510300",
            metric=AlertMetric.DAILY_CHANGE_PCT,
            direction=AlertDirection.BELOW,
            threshold=-2.0,
            channels=["feishu", "telegram"],
        )
    )

    rules = repository.list_alert_rules()

    assert created.rule_id is not None
    assert len(rules) == 1
    assert rules[0].asset_code == "510300"
    assert rules[0].channels == ["feishu", "telegram"]
