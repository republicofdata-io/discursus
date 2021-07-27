# What is the discursus project
The discursus project is an open source platform that mines, shapes and exposes the digital artifacts of protests, their discourses and the actors that influence social reforms.

[For a full intro, read here](https://www.olivierdupuis.com/introducing-discursus-io/)


# Current Status
ERD:
![ERD](resources/images/discursus_core_erd.png "ERD")

Stack:
![Stack](resources/images/discursus_core_stack.png "Stack")

dbt DAG:
![dbt DAG](resources/images/discursus_core_dbt_dag.png "dbt DAG")



# Environment
* Set environment variables
    * Rename the `Dockerfile_app.REPLACE` file to `Dockerfile_app`
    * Change the values of environment variables within
* Make any necessary changes to docker-compose
    * This depends on your docker contexts for `docker compose up`
    * Current version is to run locally

# Run Docker stack locally
* `docker compose -p "discursus-data-platform" --file docker-compose.yml up --build`

# Visit Dagster's app
* Go to `http://127.0.0.1:3000/`
