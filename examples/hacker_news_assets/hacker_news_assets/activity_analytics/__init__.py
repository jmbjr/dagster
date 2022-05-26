from hacker_news_assets.sensors.hn_tables_updated_sensor import make_hn_tables_updated_sensor

from dagster import assets_from_package_module, build_assets_job

from . import assets

activity_analytics_assets = assets_from_package_module(
    package_module=assets,
    key_prefix=["snowflake", "activity_analytics"],
    group="activity_analytics",
)


activity_analytics_assets_sensor = make_hn_tables_updated_sensor(
    # we use an asset_selection here instead of passing asset references directly, because we want
    # to include assets that are defined in dbt
    build_assets_job("story_activity_analytics_job", asset_selection="group:activity_analytics")
)

activity_analytics_definitions = [
    activity_analytics_assets,
    activity_analytics_assets_sensor,
]
