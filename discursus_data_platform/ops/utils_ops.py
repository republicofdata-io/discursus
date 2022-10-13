from dagster import op, AssetMaterialization, Output


################
# Op to materialize data asset in Dagster
@op
def materialize_data_asset(context, df_data_asset, file_path):
    context.log.info("Materializing data asset")

    s3_bucket_name = 'discursus-io'
    asset_key_parent = context.op_config["asset_key_parent"]
    asset_key_child = context.op_config["asset_key_child"]
    asset_description = context.op_config["asset_description"]
    
    # Materialize asset
    yield AssetMaterialization(
        asset_key = [asset_key_parent, asset_key_child],
        description = asset_description,
        metadata={
            "path": "s3://" + s3_bucket_name + "/" + file_path,
            "rows": df_data_asset.index.size
        }
    )
    yield Output(df_data_asset)