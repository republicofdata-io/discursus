<p align="center">
  <a href="https://www.discursus.io/">
    <img src="resources/images/discursus_logo_white.png" width="150px" alt="discursus.io" />
  </a>
</p>
<p align="center">
    <a href="https://www.discursus.io/">Website</a> |
    <a href="https://twitter.com/discursus_io">Twitter</a> |
    <a href="https://www.linkedin.com/company/discursus-data-platform/">LinkedIn</a>
</p>

# The Lantrns Analytics Open-Source Data Product Framework

The __Lantrns Analytics open-source data product framework__ provides an architecture and suite of librairies to quickly start sourcing, transforming and delivering high quality data assets to your users.

<img src='resources/images/lantrns_analytics_data_product_framework.png' width='750px' alt='Lantrns Analytics open-source data product framework' />

&nbsp;

## Implementations

We use this framework for our very own [discursus data product](https://www.discursus.io). Below is an overview of what a typical flow of data asset transformations looks like.

<img src='resources/images/discursus_data_platform.png' width='750px' alt='discursus data platform' />

&nbsp;

## Architecture

In the case above, we rely on the following architecture to support our instance.

<img src="resources/images/discursus_core_architecture.png" width="750px" alt="discursus" />

Here are the components of this implementation:

- A miner that sources events from the GDELT project and saves it to AWS S3.
- An enhancement process that scrapes the article's metadata and saves it to AWS S3.
- An ML enrichment process that classifies article's relevancy using a custom ML algorithm hosted on Novacene.ai and saves results to AWS S3.
- A suite of snowpipes that loads S3 data to Snowflake.
- A dbt project that creates a data warehouse which exposes protest events.
- A Dagster app that orchestrates all data transformation jobs and the creation of assets.

&nbsp;

## Installation
To spin up an instance of the __Lantrns Analytics open-source data product framework__, you will need the following external service accounts in place:
- An AWS S3 bucket to store raw and enhanced data assets.
- An AWS ec2 instance (or similar) to run your own instance.

In addition to those 2 foundational components, you might need the following:
- A Snowflake account (or similar) to stage data, perform transformations of data and expose entities.
- A Novacene.ai account (or similar) to perform ML enrichments.

Once you have all those in place, you can fork the this repo.

Only thing left is to configure your instance:
- Rename the `Dockerfile_app.REPLACE` file to `Dockerfile_app`.
- Change the values of environment variables within the `Dockerfile_app` file.
- Make any necessary changes to `docker-compose`
- To run the Docker stack locally: `docker compose -p "dpf-core" --file docker-compose.yml up --build`
- Visit Dagster's app: `http://127.0.0.1:3000/`

&nbsp;

# Libraries
Libraries are groups of ops to be used within your own instance of the framework. An op might require a [resource](https://docs.dagster.io/concepts/resources) and/or [configurations](https://docs.dagster.io/concepts/configuration/config-schema#run-configuration) or nothing.

## Installation
We assume you are running a Docker file such as the one we have in the [Core repo](https://github.com/lantrns-analytics/dpf_core/blob/release/0.1/Dockerfile_app.REPLACE). Let's say we wanted to add the GDELT library, the only thing you need to add is the following.

`RUN pip3 install git+https://github.com/lantrns-analytics/dpf_gdelt@release/0.1.1`

## Passing a resource
When you call an op from the library, you might need to pass a resource which is needed to run the ops.

```
from dpf_utils import persistance_ops

aws_configs = config_from_files(['configs/aws_configs.yaml'])
my_aws_client = gdelt_resources.gdelt_client.configured(aws_configs)

@job(
    resource_defs = {
        'aws_client': my_aws_client
    }
)
def my_job():
     persistance_ops.save_data_asset()
```

## Configuring ops
Some ops require you pass configurations that will shape how that op will run. Passing configuations is as simple as adding those to the job decorator. For example:

```
from dpf_gdelt import gdelt_mining_ops

@job(
    resource_defs = {
        'aws_client': my_aws_client
    },
    config = {
        "ops": {
            "get_url_to_latest_asset": {
                "config": {
                    "gdelt_asset": "events"
                }
            },
            "materialize_data_asset": {
                "config": {
                    "asset_key_parent": "sources",
                    "asset_key_child": "gdelt_events",
                    "asset_description": "List of events mined on GDELT"
                }
            },
            "filter_latest_events": {
                "config": {
                    "filter_event_code": 14,
                    "filter_countries": {
                        "US",
                        "CA"
                    }
                }
            }
        }
    }
)
def mine_gdelt_events():
    latest_events_url = gdelt_mining_ops.get_url_to_latest_asset()
    latest_events_source_path = gdelt_mining_ops.build_file_path(latest_events_url)
    df_latest_events = gdelt_mining_ops.mine_latest_asset(latest_events_url)
    df_latest_events_filtered = gdelt_mining_ops.filter_latest_events(df_latest_events)
    persistance_ops.save_data_asset(df_latest_events_filtered, latest_events_source_path)
    persistance_ops.materialize_data_asset(df_latest_events_filtered, latest_events_source_path)
```

&nbsp;

# Contributing

There are many ways you can contribute and help evolve the __Lantrns Analytics open-source data product framework__. Here a few ones:

* Star this repo, visit our [website](https://www.lantrns.co/) and follow us on [Twitter](https://twitter.com/lantrns_co).
* Fork this repo and run an instance yourself and please 🙏 help us out with documentation.
* Take ownership of some of the [issues we already documented](https://github.com/lantrns-analytics/dpf_core/issues), and send over some PRs.
* Contribute to the libraries.
* Create issues every time you feel something is missing or goes wrong.

All sort of contributions are **welcome and extremely helpful** 🙌 

&nbsp;

# License

The __Lantrns Analytics open-source data product framework__ is [MIT licensed](./LICENSE.md).
