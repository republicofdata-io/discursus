# Environment
* Set environment variables
    * Rename the Docker_xyz.REPLACE files to Docker_xyz
    * Change the values of environment variables within
* Make any necessary changes to docker-compose
    * This depends on your docker contexts for `docker compose up`
    * Current version is to run locally

# Run Docker stack
* Locally
    * `docker compose -p "dio-data-stack" --file docker-compose.yml up`
* On AWS ECS
    * Login to docker: `docker login registry.hub.docker.com`
    * Launch compose: `docker compose -p "dio-data-stack" --file docker-compose-ecs.yml up`