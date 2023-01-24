# Generate semantic definitions

## Refresh data warehouse
You first need to run the `dp_data_warehouse` data product in the platform to get a clean data warehouse environment.

- Run the data platform: `make docker`
- Materialize the assets from the `staging`, `integration` and `warehouse` groups
- Clean up the data warehouse of deprecated objects by materializing the `semantic_definitions` asset

## Data warehouse Docs
Generate field descriptions using OpenAI.

- `cd ./semantics`
- `droughty docs --profile-dir ./droughty_profiles.yml --project-dir ./droughty_projects.yml`

## Data warehouse ERD
Generate dbml definitions.

- `cd ./semantics`
- `droughty dbml --profile-dir ./droughty_profiles.yml --project-dir ./droughty_projects.yml`
- `dbdocs build dbml/discursus_analytics.dbml`
- Go to link provided as output to previous command
- Click on Relationships in top menu
- Click on Download util and export to png
- Save to `./resources/images/dw_erd.png`


# Generate platform graphs

## Asset lineage graphs
Screenshot the asset lineage graph from Dagster.

- Run the data platform locally
- Click on Assets in the top-right menu
- Click on View global asset lineage
- Screen capture the lineage
- Save to `./resources/images/asset_lineage_graph.png`

## Data Warehouse DAG
Screenshot the ETL DAG from dbt.

- `cd ./discursus_data_platform/dp_data_warehouse/`
- `dbt docs generate --profiles-dir ./`
- `dbt docs serve --profiles-dir ./`
- Once on the local dbt documentations website, click on the View Lineage Graph icon at bottom right
- Unselect the `analysis` resources
- Screen capture the DAG
- Save to `./resources/images/dw_dag.png`