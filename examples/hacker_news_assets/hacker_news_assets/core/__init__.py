from dagster import assets_from_package_module, build_assets_job, schedule_from_partitions

from . import assets


def storage_prefix_fn(asset_def):
    return {"parquet_io_manager": "s3", "warehouse_io_manager": "snowflake"}[
        asset_def.io_manager_key
    ]


core_assets = assets_from_package_module(
    package_module=assets, key_prefix=[storage_prefix_fn, "core"], group="core"
)

RUN_TAGS = {
    "dagster-k8s/config": {
        "container_config": {
            "resources": {
                "requests": {"cpu": "500m", "memory": "2Gi"},
            }
        },
    }
}

core_assets_schedule = schedule_from_partitions(
    build_assets_job("core_job", core_assets, tags=RUN_TAGS)
)


core_definitions = [core_assets, core_assets_schedule]
