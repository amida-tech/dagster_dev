from setuptools import find_packages, setup

setup(
    name="daster",
    install_requires=[
        "dagster",
        "dagster-snowflake",
        "dagster-snowflake-pandas",
        "python-dotenv",
        "dagster-cloud",
        "boto3",
        "pandas",
        "requests",
        "azure-storage-blob",
        "dagster-azure",
        "snowflake-snowpark-python",
        "snowflake-snowpark-python[pandas]",
        "cryptography",
        "dagster-azure",
        "pyarrow",
        "paramiko",
        "bs4",
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
    packages=find_packages(),
)
