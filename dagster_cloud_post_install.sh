apt-get update
apt-get install -y --no-install-recommends git
export PATH=$PATH:/usr/bin

pip install git+https://github.com/discursus-data/saf_aws.git@0.2#egg=saf-aws
pip install git+https://github.com/discursus-data/saf_gdelt.git@0.2#egg=saf-gdelt
pip install git+https://github.com/discursus-data/saf_novacene.git@0.2.1#egg=saf-novacene
pip install git+https://github.com/discursus-data/saf_web_scraper.git@0.3.1#egg=saf-web-scraper
pip install git+https://github.com/discursus-data/saf_openai.git@0.1#egg=saf-openai

cd ./discursus_data_platform/dp_data_warehouse/; 
dbt deps --profiles-dir ./config/