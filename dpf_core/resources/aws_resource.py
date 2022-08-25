from dagster import resource, StringSource

class AWSClient:
    def __init__(self, s3_bucket_name):
        self._s3_bucket_name = s3_bucket_name


    def get_s3_bucket_name(self):
        return self._s3_bucket_name


@resource(
    config_schema={
        "resources": {
            "s3": {
                "config": {
                    "bucket_name": StringSource
                }
            }
        }
    },
    description="A AWS client.",
)
def aws_client(context):
    return AWSClient(
        s3_bucket_name = context.resource_config["resources"]["s3"]["config"]["bucket_name"]
    )