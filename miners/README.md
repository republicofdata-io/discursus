# Miners
## GDELT
Pulls data from https://www.gdeltproject.org/data.html and saves it to S3

Deployment
* Set environment variables
    * Make file executable: `chmod u+x miners/gdelt/utils/set_environment_variables_fork.sh`
    * To run: `. miners/gdelt/utils/set_environment_variables_fork.sh`

* Run script: `. miners/gdelt/gdelt_miner.sh`