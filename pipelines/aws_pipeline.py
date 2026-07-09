import os
import dlt
from dlt.sources.filesystem import filesystem, read_parquet

if __name__ == "__main__":

    destination = os.getenv("DLT_DESTINATION", "filesystem")

    bucket_url = dlt.config["sources.aws_cur.bucket_url"]
    file_glob = dlt.config["sources.aws_cur.file_glob"]
    table_name = dlt.config["sources.aws_cur.table_name"]

    try:
        dataset_name = dlt.config["sources.aws_cur.dataset_name"]
    except KeyError:
        dataset_name = "aws_costs"

    try:
        pipeline_name = dlt.config["pipeline.pipeline_name"]
    except KeyError:
        pipeline_name = "aws_cost_pipeline"

    try:
        from dlt.common import pendulum
        initial_start_date_str = dlt.config["sources.aws_cur.initial_start_date"]
        initial_start_date = pendulum.parse(initial_start_date_str)
    except KeyError:
        initial_start_date = None

    cur_files = filesystem(
        bucket_url=bucket_url,
        file_glob=file_glob,
        incremental=dlt.sources.incremental("modification_date", initial_value=initial_start_date),
    )

    cur_data = cur_files | read_parquet()

    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name,
        destination=destination,
        dataset_name=dataset_name,
    )

    resource = cur_data.with_name(table_name)

    resource.apply_hints(
        primary_key=["identity_line_item_id", "identity_time_interval"],
        write_disposition="merge",
        merge_key=["identity_line_item_id", "identity_time_interval"]
    )

    if destination == "filesystem":
        load_info = pipeline.run(resource, loader_file_format="parquet")
    else:
        load_info = pipeline.run(resource)

    print(f"\nPipeline {pipeline.pipeline_name} completed successfully")
    print(f"Loaded to: {pipeline.destination}")
    print(f"Dataset: {pipeline.dataset_name}")
