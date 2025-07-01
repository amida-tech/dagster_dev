
  


amida-dlh-dagster-dev


  
  
  
  



  A cutting-edge development environment for Amida's healthcare data lakehouse, orchestrating MEDICAID and MEDICARE pipelines with Dagster and Dagger. Built on Snowflake with Iceberg tables and Azure Data Lake Storage, it leverages a medallion architecture for scalable, HIPAA-compliant data workflows.


**Overview**
The amida-dlh-dagster-dev repository powers Amida's healthcare data lakehouse, transforming raw MEDICAID and MEDICARE data into actionable insights. Using Dagster+ and Dagger, it orchestrates metadata-driven pipelines across Bronze, Silver, and Gold layers on Snowflake with Iceberg external tables and Azure Data Lake Storage (ADLS). Reusable components for data quality (DQ), ETL, and file processing ensure efficiency, while HIPAA-compliant practices safeguard sensitive data.

**Directory Structure**

utils/: Shared utilities for Snowflake, ADLS, Azure Key Vault, and HIPAA-compliant logging.
reusable_components/: Modular jobs for DQ, ETL, and file processing, driven by Snowflake metadata.
dq/: Null checks, duplicates, schema validation.
etl/: Straight moves, SCD1, SCD2, SQL execution.
file_processing/: File downloads, unzipping, format conversion.
sql_templates/: Reusable SQL snippets.


orchestration_pipelines/: End-to-end pipelines for MEDICAID/MEDICARE (e.g., Claims, Member).
medicaid/ & medicare/: Program-specific code with subject areas (Member, Provider, Claims, TPL).
assets/bronze/, assets/silver/, assets/gold/: Layer-specific Dagster assets.
sql/bronze/, sql/silver/, sql/gold/: Layer-specific SQL scripts.


docs/: Guides for structure, metadata, and components.
tests/: Unit and integration tests for reliability.

**Setup**
Follow these steps to get started:

Clone the Repository:
git clone https://github.com/<your-org>/amida-dlh-dagster-dev.git
cd amida-dlh-dagster-dev


Install Dependencies:
pip install -r requirements.txt


Configure Environment:

Set up Azure Key Vault for Snowflake and ADLS credentials.
Update utils/snowflake.py and utils/adls.py with connection settings.
Populate Snowflake metadata (e.g., METADATA.LOAD_CONFIG with source_path, target_table, sql_path).


Deploy to Dagster+:

Log in to Dagster+.
Add code locations for medicaid/repository.py and medicare/repository.py.
Deploy using the Dockerfile via GitHub Actions or Dagster+ build service.



**Usage**

Run a Pipeline:
In Dagster+ UI, select orchestration_pipelines/medicaid/claims_pipeline.py.
Launch to process Claims through Bronze (raw load), Silver (cleansing), and Gold (aggregation), with DQ checks at each stage.


Add a Subject Area:
Create medicaid/new_area/ with assets/bronze/, sql/silver/, etc.
Update medicaid/repository.py to load new assets.
Add metadata to METADATA.LOAD_CONFIG.


**Monitor:**
Use Dagster+ observability tools to track pipeline runs and data lineage.



**HIPAA Compliance**

Access Control: Restrict GitHub access to authorized teams (e.g., MEDICAID developers).
Secrets Management: Store credentials in Azure Key Vault (utils/keyvault.py).
Audit Logging: Enable HIPAA-compliant logging (utils/hipaa_logging.py) with program, subject, and layer tags.
Dagster+ Security: Leverage managed infrastructure for encryption and audit trails.

**Documentation**

Structure: See docs/structure.md for folder and code location details.
Metadata: Check docs/metadata.md for Snowflake schema (e.g., METADATA.LOAD_CONFIG).
Components: Refer to docs/components.md for using reusable components (e.g., etl/scd2.py).


