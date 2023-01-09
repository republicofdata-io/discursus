## Asset lineage graphs
- Run the data platform locally
- Click on Assets in the top-right menu
- Click on View global asset lineage
- Screen capture the lineage
- Save to `./resources/images/asset_lineage_graph.png`

## Data warehouse ERD
- `cd ./semantics`
- `droughty dbml --profile-dir ./droughty_profiles.yml --project-dir ./droughty_projects.yml`
- `dbdocs build dbml/discursus_analytics.dbml`
- Go to link provided as output to previous command
- Click on Relationships in top menu
- Click on Download util and export to png
- Save to `./resources/images/dw_erd.png`

## Data warehouse Docs
- `cd ./semantics`
- `droughty docs --profile-dir ./droughty_profiles.yml --project-dir ./droughty_projects.yml`

## Data Warehouse DAG
- `cd ./discursus_data_platform/dp_data_warehouse/`
- `dbt docs generate --profiles-dir ./`
- `dbt docs serve --profiles-dir ./`
- Once on the local dbt documentations website, click on the View Lineage Graph icon at bottom right
- Unselect the `analysis` resources
- Screen capture the DAG
- Save to `./resources/images/dw_dag.png`