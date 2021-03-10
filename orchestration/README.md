# About
Dagster workspace, pipeline and solids to orchestrate building of discursus data


# Deployment
## Deployment Environment
* ec2: `ssh -i ~/Dropbox/Technical/AWS/discursus_io.pem ec2-user@54.88.102.177`
* Preparing the environment
  * For Dagit to run, it needs a `DAGSTER_HOME` variable that points to a local directory. For example: `~/dagster/dagster_home` 
  * Solids are dependant on resources which are configured through the `environments/shared.yaml` variables. 
  * Those configurations are dependant on environment variables which can be setup using the a local fork of the `utils/set_environment_variables.sh` bash script. 
    * Make file executable: `chmod u+x utils/set_environment_variables_fork.sh`
    * To run: `. utils/set_environment_variables_fork.sh`


## Dagit
* Start server
  * Run Dagit from EC2 instance: `DAGSTER_HOME=~/dagster/dagster_home nohup dagit -h 0.0.0.0 -p 3000 > dagit.out &`
* Start scheduler daemon
  * Run from EC2 instance: `DAGSTER_HOME=~/dagster/dagster_home nohup dagster-daemon run > dagster_daemon.out &` 
* To kill processes
  * Find process associated to command: `ps aux | grep dagit` and `ps aux | grep dagster-daemon`
  * Kill process if needed: `kill -9 25177`


## Maintenance
* TO upgrade packages
  * `pip install dagster dagit dagster_shell dagster_snowflake dagster_dbt --upgrade`