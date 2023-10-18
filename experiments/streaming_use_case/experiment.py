import random
import requests
import concurrent.futures
import time
import uuid
from datetime import datetime
import pytz


marquez_url = "http://localhost:5000/api/v1"
infinite_counter = [i for i in range(1000)]
parallel_jobs = 10
parallel_jobs_list = [i for i in range(parallel_jobs)]
run_ids = [ str(uuid.uuid4()) for i in range(parallel_jobs) ]


def get_now_formatted():
    current_timestamp = datetime.now(tz=pytz.timezone("America/Sao_Paulo")).strftime("%Y-%m-%dT%H:%M:%S.%f%z")
    return current_timestamp[:-8]+current_timestamp[-5:-2]+":"+current_timestamp[-2:]


def hit_marquez(infinite_counter):
    random_job_id = random.randint(1, parallel_jobs-1)
    running_event = create_event_running_run(random_job_id)

    try:
        response = requests.post(url=f'{marquez_url}/lineage',json=running_event)
    except Exception as e:
        print(f'An Exception happend: {e}')
    else:
        print(f'request {infinite_counter}, running job: {random_job_id}, response status code: {response.status_code}')


def send_concurrent_requests(infinite_counter):
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(parallel_jobs_list)) as executor:
        executor.map(hit_marquez, infinite_counter)
    

def create_event_start_run(job):
    return {
        "eventTime": get_now_formatted(),
        "producer": "https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client",
        "schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json#/definitions/RunEvent",

        "eventType": "START",

        "run": {
            "runId": run_ids[job],
            "facets": {
                "nominalTime": {
                    "_producer": "https://github.com/OpenLineage/OpenLineage/tree/0.10.0/integration/airflow",
                    "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/NominalTimeRunFacet",
                    "nominalStartTime": get_now_formatted()
                }
            }
        },
        "job": {
            "namespace": "namespace_1",
            "name": f"streaming_job_{job}",
            "type": "STREAM",
            "facets": {
                "ownership": {
                    "_producer": "https://some.producer.com/version/1.0",
                    "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/OwnershipJobFacet.json",
                    "owners": [
                        {
                            "name": "lyamada",
                            "type": "MAINTAINER"
                        }
                    ]
                },
                "documentation": {
                    "_producer": "https://some.producer.com/version/1.0",
                    "_schemaURL": "https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/DocumentationJobFacet.json",
                    "description": f"This is a Streaming Job known as {job}"
                },
                "sourceCode": {
                    "_producer": "https://some.producer.com/version/1.0",
                    "_schemaURL": "https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/SourceCodeJobFacet.json",
                    "language": "python",
                    "sourceCode": ""
                },
                "sourceCodeLocation": {
                    "_producer": "https://some.producer.com/version/1.0",
                    "_schemaURL": "https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/SourceCodeLocationJobFacet.json",
                    "type": "git",
                    "url": "https://github.com/lmassaoy/marquez-experiments/blob/main/experiments/streaming_use_case/experiment.py",
                    "repoUrl": "https://github.com/lmassaoy/marquez-experiments.git",
                    "path": "experiments/streaming_use_case/",
                    "version": "https://github.com/lmassaoy/marquez-experiments/commit/0c79e8bfe4b406f80f096510b33ee42e466f0e28",
                    "tag": "some_nice_tag",
                    "branch": "main"
                },
                "sql": {
                    "_producer": "https://some.producer.com/version/1.0",
                    "_schemaURL": "https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/SQLJobFacet.json",
                    "query": "SELECT event_id, event_timestamp, transaction_type, value FROM banking_transactions"
                }
            }
        },
        "inputs": [{
            "namespace": "namespace_1",
            "name": "banking_transactions",
            "physicalName": "kafka_topic.banking_transactions",
            "sourceName": "banking_transactions",
            "type": "KAFKA_TOPIC",
            "facets": {
                "schema": {
                    "_producer": "https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client",
                    "_schemaURL": "https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/spec/OpenLineage.json#/definitions/SchemaDatasetFacet",
                    "fields": [
                        { "name": "event_id", "type": "VARCHAR", "description": "this is the column EVENT ID"},
                        { "name": "event_timestamp", "type": "TIMESTAMP", "description": "this is the column EVENT TIMESTAMP"},
                        { "name": "transaction_type", "type": "INTEGER", "description": "this is the column TRANSACTION TYPE"},
                        { "name": "value", "type": "DOUBLE", "description": "this is the column VALUE"}
                    ]
                },
                "dataSource": {
                    "_producer": "https://some.producer.com/version/1.0",
                    "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/DatasourceDatasetFacet.json",
                    "name": "kafka_topic.banking_transactions",
                    "uri": f"kafka_broker:9092/topic_{job}",
                    "description": "This is a Kafka Topic"
                }
            }
        }]
    }


def create_event_running_run(job):
    return {
        "eventTime": get_now_formatted(),
        "producer": "https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client",
        "schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json#/definitions/RunEvent",

        "eventType": "RUNNING",

        "run": {
            "runId": run_ids[job],
            "facets": {
                "nominalTime": {
                    "_producer": "https://github.com/OpenLineage/OpenLineage/tree/0.10.0/integration/airflow",
                    "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/NominalTimeRunFacet",
                    "nominalStartTime": get_now_formatted()
                }
            }
        },
        "job": {
            "namespace": "namespace_1",
            "name": f"streaming_job_{job}",
            "type": "STREAM",
            "facets": {
                "ownership": {
                    "_producer": "https://some.producer.com/version/1.0",
                    "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/OwnershipJobFacet.json",
                    "owners": [
                        {
                            "name": "lyamada",
                            "type": "MAINTAINER"
                        }
                    ]
                },
                "documentation": {
                    "_producer": "https://some.producer.com/version/1.0",
                    "_schemaURL": "https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/DocumentationJobFacet.json",
                    "description": f"This is a Streaming Job known as {job}"
                },
                "sourceCode": {
                    "_producer": "https://some.producer.com/version/1.0",
                    "_schemaURL": "https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/SourceCodeJobFacet.json",
                    "language": "python",
                    "sourceCode": ""
                },
                "sourceCodeLocation": {
                    "_producer": "https://some.producer.com/version/1.0",
                    "_schemaURL": "https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/SourceCodeLocationJobFacet.json",
                    "type": "git",
                    "url": "https://github.com/lmassaoy/marquez-experiments/blob/main/experiments/streaming_use_case/experiment.py",
                    "repoUrl": "https://github.com/lmassaoy/marquez-experiments.git",
                    "path": "experiments/streaming_use_case/",
                    "version": "https://github.com/lmassaoy/marquez-experiments/commit/0c79e8bfe4b406f80f096510b33ee42e466f0e28",
                    "tag": "some_nice_tag",
                    "branch": "main"
                },
                "sql": {
                    "_producer": "https://some.producer.com/version/1.0",
                    "_schemaURL": "https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/SQLJobFacet.json",
                    "query": "SELECT event_id, event_timestamp, transaction_type, value FROM banking_transactions"
                }
            }
        },
        "outputs": [{
            "namespace": "namespace_1",
            "name": f"banking_transactions_v{job}",
            "physicalName": f's3://my_bucket/banking_transactions_v{job}',
            "type": "FILE",
            "facets": {
                "schema": {
                    "_producer": "https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client",
                    "_schemaURL": "https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/spec/OpenLineage.json#/definitions/SchemaDatasetFacet",
                    "fields": [
                        { "name": "event_id", "type": "VARCHAR", "description": "this is the column EVENT ID"},
                        { "name": "event_timestamp", "type": "TIMESTAMP", "description": "this is the column EVENT TIMESTAMP"},
                        { "name": "transaction_type", "type": "INTEGER", "description": "this is the column TRANSACTION TYPE"},
                        { "name": "value", "type": "DOUBLE", "description": "this is the column VALUE"}
                    ]
                },
                "columnLineage": {
                    "_producer": "https://github.com/MarquezProject/marquez/blob/main/docker/metadata.json",
                    "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/ColumnLineageDatasetFacet.json",
                    "fields": {
                        "event_id": {
                            "inputFields": [
                                {
                                    "namespace": "namespace_1",
                                    "name": "banking_transactions",
                                    "field": "event_id"
                                }
                            ],
                            "transformationDescription": "",
                            "transformationType": "SQL"
                        },
                        "event_timestamp": {
                            "inputFields": [
                                {
                                    "namespace": "namespace_1",
                                    "name": "banking_transactions",
                                    "field": "event_timestamp"
                                }
                            ],
                            "transformationDescription": "",
                            "transformationType": "SQL"
                        },
                        "transaction_type": {
                            "inputFields": [
                                {
                                    "namespace": "namespace_1",
                                    "name": "banking_transactions",
                                    "field": "transaction_type"
                                }
                            ],
                            "transformationDescription": "",
                            "transformationType": "SQL"
                        },
                        "value": {
                            "inputFields": [
                                {
                                    "namespace": "namespace_1",
                                    "name": "banking_transactions",
                                    "field": "value"
                                }
                            ],
                            "transformationDescription": "",
                            "transformationType": "SQL"
                        }
                    }
                }
            }
        }]
    }


for job in parallel_jobs_list:
    start_job_event = create_event_start_run(job)

    try:
        response = requests.post(url=f'{marquez_url}/lineage',json=start_job_event)
    except Exception as e:
        print(f'An Exception happend: {e}')
    else:
        print(f'starting run: {run_ids[job]}, response status code: {response.status_code}')
    # print(start_job_event)


send_concurrent_requests(infinite_counter)


for job in parallel_jobs_list:
    complete_job_event = create_event_running_run(job)
    complete_job_event['eventType'] = 'COMPLETE'

    try:
        response = requests.post(url=f'{marquez_url}/lineage',json=complete_job_event)
    except Exception as e:
        print(f'An Exception happend: {e}')
    else:
        print(f'ending run: {run_ids[job]}, response status code: {response.status_code}')