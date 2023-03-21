from setuptools import find_packages, setup

if __name__ == "__main__":
    setup(
        name="discursus_data_platform",
        packages=find_packages(),
        install_requires=[
            "pandas>=1.5.2",
            "dagster==1.2.2",
            "dagster-cloud==1.2.2",
            "dagster_snowflake==0.18.2",
            "dagster-dbt==0.18.2",
            "dagster-aws==0.18.2",
            "dagster-pandas==0.18.2",
            "dagster-hex==0.1.2",
            "dbt-core==1.4.5",
            "dbt-snowflake==1.4.2"
        ]
    )
