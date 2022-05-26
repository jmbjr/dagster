from hacker_news_assets.core import core_assets
from hacker_news_assets.sensors.hn_tables_updated_sensor import make_hn_tables_updated_sensor

from dagster import assets_from_package_module, build_assets_job

from . import assets


def storage_prefix_fn(asset_def):
    return {"warehouse_io_manager": "snowflake"}.get(asset_def.io_manager_key, "s3")


recommender_assets = assets_from_package_module(
    package_module=assets,
    extra_source_assets=core_assets.to_source_assets(),
    key_prefix=[storage_prefix_fn, "recommender"],
    group="recommender",
)

recommender_assets_sensor = make_hn_tables_updated_sensor(
    build_assets_job("story_recommender_job", recommender_assets)
)

recommender_definitions = [*recommender_assets, recommender_assets_sensor]
