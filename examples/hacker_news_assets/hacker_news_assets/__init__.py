import json
import os

from dagster_dbt.asset_defs import load_assets_from_dbt_manifest
from hacker_news_assets.activity_analytics import activity_analytics_definitions
from hacker_news_assets.core import core_definitions
from hacker_news_assets.recommender import recommender_definitions
from hacker_news_assets.resources import RESOURCES_LOCAL, RESOURCES_PROD, RESOURCES_STAGING

from dagster import repository, with_resources
from dagster.utils import file_relative_path

from .sensors.slack_on_failure_sensor import make_slack_on_failure_sensor

DBT_PROJECT_DIR = file_relative_path(__file__, "../dbt")
DBT_PROFILES_DIR = DBT_PROJECT_DIR + "/config"

dbt_assets = load_assets_from_dbt_manifest(
    json.load(open(os.path.join(DBT_PROJECT_DIR, "target", "manifest.json"), encoding="utf-8")),
    io_manager_key="warehouse_io_manager",
    # the schemas are already specified in dbt, so we don't need to also specify them in the key
    # prefix here
    key_prefix=["snowflake"],
    group="activity_analytics",
)


@repository
def prod():
    return with_resources(
        [
            *core_definitions,
            *recommender_definitions,
            *activity_analytics_definitions,
            make_slack_on_failure_sensor(base_url="my_dagit_url"),
        ],
        RESOURCES_PROD,
    )


@repository
def staging():
    return with_resources(
        [
            *core_definitions,
            *recommender_definitions,
            *activity_analytics_definitions,
            make_slack_on_failure_sensor(base_url="my_dagit_url"),
        ],
        RESOURCES_STAGING,
    )


@repository
def local():
    return with_resources(
        [
            *core_definitions,
            *recommender_definitions,
            *activity_analytics_definitions,
        ],
        RESOURCES_LOCAL,
    )
