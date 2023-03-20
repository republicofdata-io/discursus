apt-get update
apt-get install -y --no-install-recommends git

cd ./discursus_data_platform/dp_data_warehouse/; 
dbt deps --profiles-dir ./config/