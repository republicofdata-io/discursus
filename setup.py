from setuptools import find_packages, setup

if __name__ == "__main__":
    setup(
        name="discursus_data_platform",
        packages=find_packages(),
        install_requires=[
            "sqlalchemy==1.4.46",
            "future>=0.18.2,<1.0.0",
            "dbt-core==1.4.5",
            "dbt-snowflake==1.4.2",
            "dagster==1.3.5",
            "dagit==1.3.5",
            "dagster-cloud==1.3.5",
            "dagster-graphql==1.3.5",
            "dagster-postgres==0.19.5",
            "dagster-docker==0.19.5",
            "dagster-shell==0.19.5",
            "dagster-pandas==0.19.5",
            "dagster-dbt==0.19.5",
            "dagster-aws==0.19.5",
            "dagster-snowflake==0.19.5",
            "dagster-hex==0.1.2",
            "pandas>=1.5.2",
            "google-cloud-bigquery>=3.10.0",
            "db-dtypes>=1.1.1",
            "bs4>=0.0.1,<1.0.0",
            "optparse-pretty>=0.1.1,<1.0.0",
            "boto3>=1.26.35,<2.0.0",
            "spacy>=3.5.2",
            "fsspec>=2022.11.0,<2023.0.0",
        ]
    )
