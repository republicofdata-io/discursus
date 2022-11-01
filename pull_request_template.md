# Description

Please include a summary of the change and which issue is fixed. Please also include relevant motivation and context. List any dependencies that are required for this change.

Fixes # (issue)

# Lineage changes

Are there any changes to the lineage of assets produced? If so, please include graphs that clarfies where the changes are.

# Type of change

Please select option that is relevant.

- [ ] New feature (adds functionality)
- [ ] Bug fix (fixes an issue)
- [ ] Infrastructure (provides the necessary resources for platform to run)

# Checklist:

- [ ] My pull request represents one story (logical piece of work).
- [ ] I have performed a self-review of my own code
- [ ] I have commented my code, particularly in hard-to-understand areas
- [ ] I ran the data platform in my development environment without error.
- [ ] I ran the data pipelines in my development environment without error.
- [ ] I updated the README file with up-to-date asset lineage graphs, data wareohouse DAG and ERD

## To produce README graphs
Asset lineage graphs
- Run the data platform locally
- Click on Assets in the top-right menu
- Click on View global asset lineage
- Screen capture the lineage
- Save to `./resources/images/asset_lineage_graph.png`

Data warehouse ERD
- `cd ./semantics`
- `droughty dbml --profile-dir ./droughty_profiles.yml --project-dir ./droughty_projects.yml`
- `dbdocs build dbml/discursus_analytics.dbml`
- Go to link provided as output to previous command
- Click on Relationships in top menu
- Click on Download util and export to png
- Save to `./resources/images/dw_erd.png`

Data Warehouse DAG
- `cd ./discursus_data_platform/dw/`
- `dbt docs generate --profiles-dir ./`
- `dbt docs serve --profiles-dir ./`
- Once on the local dbt documentations website, click on the View Lineage Graph icon at bottom right
- Unselect the `analysis` resources
- Screen capture the DAG
- Save to `./resources/images/dw_dag.png`