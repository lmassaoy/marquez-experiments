from openlineage.client.run import (
    RunEvent,
    RunState,
    Run,
    Job,
    Dataset,
    OutputDataset,
    InputDataset,
)
from openlineage.client.client import OpenLineageClient, OpenLineageClientOptions
from openlineage.client.facet import (
    SqlJobFacet,
    SchemaDatasetFacet,
    SchemaField,
    OutputStatisticsOutputDatasetFacet,
    SourceCodeJobFacet,
    SourceCodeLocationJobFacet,
    NominalTimeRunFacet,
    DataQualityMetricsInputDatasetFacet,
    ColumnMetric,
    DocumentationJobFacet,
    JobTypeJobFacet,
    BaseFacet,
    StorageDatasetFacet
)
import uuid
from datetime import datetime, timezone, timedelta
import time
from random import random


PRODUCER = f"https://github.com/openlineage-user"
namespace = "y-data-domain.experiments"
dag_name = "user_trends"
base_facet = BaseFacet()

url = "http://k8s-marquez-marquez-1f83396143-232926722.us-east-1.elb.amazonaws.com/"
# api_key = "1234567890ckcu028rzu5l"

client = OpenLineageClient(
    url=url,
    options=OpenLineageClientOptions(),
)


def generate_base_facet_dict():
    facet_aux = BaseFacet()
    return {
        "_producer": facet_aux._producer,
        "_schemaURL": facet_aux._schemaURL,
    }


def generate_base_facet():
    return {
        "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.6.2/client/python",
        "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/BaseFacet"
    }


# generates job facet
def job(job_name, sql, location, description=None):
    '''
    required facets:
    [X] SqlJobFacet
    [X] Documentation (description)
    [X] SourceCodeJob
    [X] SourceCodeLocation
    [X] JobTypeJobFacet
    [X] BaseFacet > AdditionalMetadata
    '''
    facets = {"sql": SqlJobFacet(sql)}
    
    if description is not None: facets.update({"documentation": DocumentationJobFacet(description)})

    # facets.update({"jobType": JobTypeJobFacet("BATCH", "OPENLINEAGE_PYTHON_CLIENT", "TASK")})
    job_type = generate_base_facet()
    job_type["processingType"] = "BATCH"
    job_type["integration"] = "OPENLINEAGE_PYTHON_CLIENT"
    job_type["jobType"] = "TASK"
    facets.update({"jobType": job_type})
    
    facets.update({"sourceCode": SourceCodeJobFacet("PYTHON", sql)})
    
    additional_metadata = generate_base_facet()
    additional_metadata["nurn"] = f"nu:data:job:{job_name}"
    facets.update({"additionalMetadata": additional_metadata})

    # if location is not None: facets.update({"sourceCodeLocation": SourceCodeLocationJobFacet("git", location)})
    source_code_location = generate_base_facet()
    source_code_location["type"] = "git"
    source_code_location["url"] = location if location is not None else job_name
    source_code_location["repoUrl"] = "https://github.com/nubank/nu-invest-palpatine.git"
    source_code_location["path"] = "contracts/"
    source_code_location["version"] = "1"
    source_code_location["tag"] = "1.0.0"
    source_code_location["branch"] = "main"
    facets.update({"sourceCodeLocation": source_code_location})

    return Job(namespace=namespace, name=job_name, facets=facets)


# geneartes run racet
def run(run_id, hour):
    return Run(
        runId=run_id,
        facets={
            "nominalTime": NominalTimeRunFacet(
                nominalStartTime=f"2022-04-14T{twoDigits(hour)}:12:00Z"
            )
        },
    )


# generates dataset
def dataset(name, schema=None, ns=namespace):
    if schema == None:
        facets = {}
    else:
        facets = {"schema": schema}

    
    storage_ds_facet = {
        "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.6.2/client/python",
        "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/StorageDatasetFacet",
        "fileFormat": "parquet",
        "storageLayer": "delta",
        "location": { 
            "type": "S3",
            "name": "ds_bucket",
            "path": "/credit/transactions/"
        }
    }
    facets.update(
        {"storage": storage_ds_facet}
    )
    return Dataset(namespace, name, facets)


# generates output dataset
def outputDataset(dataset, stats):
    output_facets = {"stats": stats, "outputStatistics": stats}
    return OutputDataset(dataset.namespace, dataset.name, dataset.facets, output_facets)


# generates input dataset
def inputDataset(dataset, dq):
    input_facets = {
        "dataQuality": dq,
    }
    return InputDataset(dataset.namespace, dataset.name, dataset.facets, input_facets)


def twoDigits(n):
    if n < 10:
        result = f"0{n}"
    elif n < 100:
        result = f"{n}"
    else:
        raise f"error: {n}"
    return result


now = datetime.now(timezone.utc)


# generates run Event
def runEvents(job_name, sql, inputs, outputs, hour, min, location, duration, description):
    run_id = str(uuid.uuid4())
    myjob = job(job_name, sql, location, description)
    myrun = run(run_id, hour)
    st = now + timedelta(hours=hour, minutes=min, seconds=20 + round(random() * 10))
    end = st + timedelta(minutes=duration, seconds=20 + round(random() * 10))
    started_at = st.isoformat()
    ended_at = end.isoformat()
    return (
        RunEvent(
            eventType=RunState.START,
            eventTime=started_at,
            run=myrun,
            job=myjob,
            producer=PRODUCER,
            inputs=inputs,
            outputs=outputs,
        ),
        RunEvent(
            eventType=RunState.COMPLETE,
            eventTime=ended_at,
            run=myrun,
            job=myjob,
            producer=PRODUCER,
            inputs=inputs,
            outputs=outputs,
        ),
    )


# add run event to the events list
def addRunEvents(
    events, job_name, sql, inputs, outputs, hour, minutes, location=None, duration=2, description=None
):
    (start, complete) = runEvents(
        job_name, sql, inputs, outputs, hour, minutes, location, duration, description
    )
    events.append(start)
    events.append(complete)


events = []

# create dataset data
for i in range(0, 1):

    user_counts = dataset("y-experiments.user_counts")
    user_history = dataset(
        "experiments.user_history",
        SchemaDatasetFacet(
            fields=[
                SchemaField(name="id", type="BIGINT", description="the user id"),
                SchemaField(
                    name="email_domain", type="VARCHAR", description="the user id"
                ),
                SchemaField(name="status", type="BIGINT", description="the user id"),
                SchemaField(
                    name="created_at",
                    type="DATETIME",
                    description="date and time of creation of the user",
                ),
                SchemaField(
                    name="updated_at",
                    type="DATETIME",
                    description="the last time this row was updated",
                ),
                SchemaField(
                    name="fetch_time_utc",
                    type="DATETIME",
                    description="the time the data was fetched",
                ),
                SchemaField(
                    name="load_filename",
                    type="VARCHAR",
                    description="the original file this data was ingested from",
                ),
                SchemaField(
                    name="load_filerow",
                    type="INT",
                    description="the row number in the original file",
                ),
                SchemaField(
                    name="load_timestamp",
                    type="DATETIME",
                    description="the time the data was ingested",
                ),
            ]
        ),
        "snowflake://",
    )

    create_user_counts_sql = """
CREATE OR REPLACE TABLE TMP_DEMO.USER_COUNTS AS (
    SELECT DATE_TRUNC(DAY, created_at) date, COUNT(id) as user_count
    FROM TMP_DEMO.USER_HISTORY
    GROUP BY date
)"""

    # location of the source code
    location = "https://github.com/some/airflow/dags/example/user_trends.py"

    # description of the job
    description = "This job counts users per date."

    # run simulating Airflow DAG with snowflake operator
    addRunEvents(
        events,
        dag_name + ".create_user_counts",
        create_user_counts_sql,
        [user_history],
        [user_counts],
        i,
        11,
        location,
        description=description
    )


for event in events:
    from openlineage.client.serde import Serde

    # print(event)
    # print(Serde.to_json(event))
    print(f"sending event...")
    time.sleep(2)
    client.emit(event)
    print(f"done!")