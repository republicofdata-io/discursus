# Orchestration
## Deployment Environment
* Preparing the environment
  * For Dagit to run, it needs a `DAGSTER_HOME` variable that points to a local directory. For example: `~/dagster/dagster_home`. So have that directory created before moving ahead.
  * Solids are dependant on resources which are configured through the `configs/prod.yaml` variables. 
  * Those configurations are dependant on environment variables which were set in the deployment instructions at the root of this project.


## Dagit
* Start server
  * `cd orchestration/`
  * Run Dagit from EC2 instance: `DAGSTER_HOME=~/dagster/dagster_home nohup dagit -h 0.0.0.0 -p 3000 > dagit.out &`
* Start scheduler daemon
  * Run from EC2 instance: `DAGSTER_HOME=~/dagster/dagster_home nohup dagster-daemon run > dagster_daemon.out &` 
* To kill processes
  * Find process associated to command: `ps aux | grep dagit` and `ps aux | grep dagster-daemon`
  * Kill process if needed: `kill -9 25177`


## Maintenance
* TO upgrade packages
  * `pip install dagster dagit dagster_shell dagster_snowflake dagster_dbt --upgrade`