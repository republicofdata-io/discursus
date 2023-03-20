from setuptools import find_packages, setup

if __name__ == "__main__":
    setup(
        name="ra_internal_analytics",
        packages=find_packages(),
        install_requires=[
            "dagster==1.2.2",
            "dagster-cloud==1.2.2",
            "dagster-dbt==0.18.2",
            "dbt-core==1.4.5",
            "dbt-snowflake==1.4.2"
        ],
    )
