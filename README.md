# Prod specific instructions
* Install docker and docker-compose
* Add ec2-user to docker group - https://docs.docker.com/engine/install/linux-postinstall/
* Start Docker: `sudo systemctl start docker`
* Run stack `nohup docker-compose -p "dio-data-stack" --file docker-compose.yml up --build > dio_stack_log.out &`

# Environment
* Set environment variables
    * Rename the `Dockerfile_pipelines.REPLACE` file to `Dockerfile_pipelines`
    * Change the values of environment variables within
* Make any necessary changes to docker-compose
    * This depends on your docker contexts for `docker compose up`
    * Current version is to run locally

# Run Docker stack locally
* `docker compose -p "discursus-data-stack" --file docker-compose.yml up --build`

# Visit Dagster's app
* Go to `http://127.0.0.1:3000/`
