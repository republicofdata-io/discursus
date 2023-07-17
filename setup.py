from setuptools import find_packages, setup

if __name__ == "__main__":
    setup(
        name="discursus_data_platform",
        packages=find_packages(),
        install_requires=[
            "boto3>=1.26.35,<2.0.0",
            "beautifulsoup4>=4.12.2",
            "dagit==1.3.14",
            "dagster==1.3.14",
            "dagster-aws==0.19.14",
            "dagster-cloud==1.3.14",
            "dagster-dbt==0.19.14",
            "dagster-docker==0.19.14",
            "dagster-graphql==1.3.14",
            "dagster-hex==0.1.3",
            "dagster-pandas==0.19.14",
            "dagster-postgres==0.19.14",
            "dagster-shell==0.19.14",
            "dagster-snowflake==0.19.14",
            "db-dtypes>=1.1.1",
            "dbt-core==1.5.2",
            "dbt-snowflake==1.5.2",
            "fsspec>=2022.11.0,<2023.0.0",
            "future>=0.18.2,<1.0.0",
            "google-cloud-bigquery>=3.10.0",
            "optparse-pretty>=0.1.1,<1.0.0",
            "pandas>=1.5.2",
            "spacy>=3.5.2",
            "sqlalchemy==1.4.46",
        ]
    )
