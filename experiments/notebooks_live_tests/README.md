# Notebook experiments (Spark-related)

## Setup & Running
Build without cache
```
docker-compose build --no-cache
```
Turning the Jupyter on
```
docker-compose up
```
Use the URL in the logs to access the Jupyter Labs

## Discovery Notes abour running OpenLineage Spark agent
- A Spark job produces multiple OpenLineage jobs, based on the internal Spark operations. Concatenating the job name (Spark App Name or the config [spark.openlineage.appName]((https://github.com/OpenLineage/OpenLineage/tree/main/integration/spark#general))) + "." + the internal operation.
  - <`job_name`>.collect_limit
  - <`job_name`>.take_ordered_and_project --> apparently it happens when we perform a sort in the dataframe
  - <`job_name`>.execute_insert_into_hadoop_fs_relation_command --> when the job writes the parquet
- Job Runs > Facets > includes interesting attributes such as:
  - `environment-properties`
  - `spark.logicalPlan`
  - `spark_version` --> Spark version + OpenLineage agent version
  - `processing_engine`
  - `spark_properties` --> natively includes properties such as `spark.master` and `spark.app.name`. If we require more properties, we must include the whole set in the config [spark.openlineage.capturedProperties](https://github.com/OpenLineage/OpenLineage/tree/main/integration/spark#general)
- Job Runs > outputDatasetVersions > facets > includes `outputStatistics` presenting interesting numbers such as `size` and `rowCount`
- Job attributes **we're missing** - need to asess how/which spark properties are related to producing the following:
  - `location` --> github URL
  - `description` --> job description
  - `documentation` --> job's documentation (likely to be custom)
  - `sourceCodeLocation` --> similar to `location`
  - `sql` --> sql logic for job's transformation
  - `ownership` --> who owns this job
  - `sourceCode` --> code logic the job uses for producing a dataset
- Regular Spark agent produces column lineage, **but doesn't include transformation logic, only columns relationship**
- Regular Spark agent doesn't include metadata such as `job description`, `ownership`, etc.
- Regular Spark agent doesn't produce detailed schemas when we're dealing with advanced data types such as `arrays` and `maps` (e.g., `array[long]`)