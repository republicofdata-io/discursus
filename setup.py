from setuptools import find_packages, setup

if __name__ == "__main__":
    setup(
        name="discursus_data_platform",
        packages=find_packages(),
        install_requires=[
            "dbt-core==1.4.5",
            "dbt-snowflake==1.4.2",
            "dagster==1.3.3",
            "dagit==1.3.3",
            "dagster-cloud==1.3.3",
            "dagster-graphql==1.3.3",
            "dagster-postgres==0.19.3",
            "dagster-docker==0.19.3",
            "dagster-shell==0.19.3",
            "dagster-pandas==0.19.3",
            "dagster-dbt==0.19.3",
            "dagster-aws==0.19.3",
            "dagster-snowflake==0.19.3",
            "dagster-hex==0.1.2",
            "sqlalchemy==1.4.46",
            "pandas>=1.5.2,<2.0.0",
            "bs4>=0.0.1,<1.0.0",
            "optparse-pretty>=0.1.1,<1.0.0",
            "boto3>=1.26.35,<2.0.0",
            "spacy>=3.5.2",
            "fsspec>=2022.11.0,<2023.0.0",
            "tweepy>=4.12.1,<5.0.0",
            "future>=0.18.2,<1.0.0",
        ]
    )
