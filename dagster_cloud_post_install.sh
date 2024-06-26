apt-get update
apt-get install -y --no-install-recommends git
export PATH=$PATH:/usr/bin

pip install git+https://github.com/discursus-data/saf_aws.git@0.2#egg=saf-aws
pip install git+https://github.com/discursus-data/saf_gdelt.git@0.2.1#egg=saf-gdelt
pip install git+https://github.com/discursus-data/saf_openai.git@0.1#egg=saf-openai

python -m spacy download en_core_web_sm

cd ./discursus_data_platform/dp_data_warehouse/; 
dbt deps --profiles-dir ./config/