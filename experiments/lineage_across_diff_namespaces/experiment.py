import time
import requests
import uuid
from datetime import datetime
import pytz


def get_now_formatted():
    current_timestamp = datetime.now(tz=pytz.timezone("America/Sao_Paulo")).strftime("%Y-%m-%dT%H:%M:%S.%f%z")
    return current_timestamp[:-8]+current_timestamp[-5:-2]+":"+current_timestamp[-2:]


marquez_url = "http://localhost:5000/api/v1"
# marquez_url = "http://k8s-marquez-marquez-1f83396143-412721712.us-east-1.elb.amazonaws.com/api/v1"


run_ids = [ str(uuid.uuid4()) for i in range(5) ]

namespaces = [
    "namespace_1", "namespace_2", "namespace_3", "namespace_4", "namespace_5"
]
job_names = [
    "job_a", "job_b", "job_c", "job_d", "job_e"
]
datasets = [
   "renewed.dataset_a", "renewed.dataset_b", "renewed.dataset_c",  "renewed.dataset_d",  "renewed.dataset_e"
]

event_a_start_run = {
    "eventTime": get_now_formatted(),
    "producer": "https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client",
    "schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json#/definitions/RunEvent",

    "eventType": "START",

    "run": {
        "runId": run_ids[0],
        "facets": {
            "nominalTime": {
                "_producer": "https://github.com/OpenLineage/OpenLineage/tree/0.10.0/integration/airflow",
                "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/NominalTimeRunFacet",
                "nominalStartTime": get_now_formatted()
            }
        }
    },
    "job": {
        "namespace": namespaces[0],
        "name": job_names[0],
        "type": "SERVICE",
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
                "description": "Job A reads the source database.table and produces dataset_a."
            },
            "sourceCode": {
                "_producer": "https://some.producer.com/version/1.0",
                "_schemaURL": "https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/SourceCodeJobFacet.json",
                "language": "python",
                "sourceCode": """
                    my_df = spark.sql('SELECT upper(a), upper(b), upper(c), upper(d), upper(e) FROM database_abc.table_abc')
                    my_df.coalesce(1).write.parquet("dataset_a")
                """
            },
            "sourceCodeLocation": {
                "_producer": "https://some.producer.com/version/1.0",
                "_schemaURL": "https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/SourceCodeLocationJobFacet.json",
                "type": "git",
                "url": "https://github.com/lmassaoy/marquez-experiments/blob/main/experiments/lineage_across_diff_namespaces/experiment.py",
                "repoUrl": "https://github.com/lmassaoy/marquez-experiments.git",
                "path": "experiments/lineage_across_diff_namespaces/",
                "version": "https://github.com/lmassaoy/marquez-experiments/commit/0c79e8bfe4b406f80f096510b33ee42e466f0e28",
                "tag": "some_nice_tag",
                "branch": "main"
            },
            "sql": {
                "_producer": "https://some.producer.com/version/1.0",
                "_schemaURL": "https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/SQLJobFacet.json",
                "query": "SELECT upper(a), upper(b), upper(c), upper(d), upper(e) FROM database_abc.table_abc"
            }
        }
    },
    "inputs": [{
        "namespace": namespaces[0],
        "name": "table_abc",
        "physicalName": "database_abc.table_abc",
        "sourceName": "table_abc",
        "type": "DB_TABLE",
        "facets": {
            "schema": {
                "_producer": "https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client",
                "_schemaURL": "https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/spec/OpenLineage.json#/definitions/SchemaDatasetFacet",
                "fields": [
                    { "name": "a", "type": "VARCHAR", "description": "this is the column A"},
                    { "name": "b", "type": "INTEGER", "description": "this is the column B"},
                    { "name": "c", "type": "INTEGER", "description": "this is the column C"},
                    { "name": "d", "type": "DOUBLE", "description": "this is the column D"},
                    { "name": "e", "type": "TIMESTAMP", "description": "this is the column E"}
                ]
            },
            "dataSource": {
                "_producer": "https://some.producer.com/version/1.0",
                "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/DatasourceDatasetFacet.json",
                "name": "database_abc.table_abc",
                "uri": "jdbc:mysql://localhost:3306/database_abc"
            }
        }
    }]
}

try:
    response = requests.post(url=f'{marquez_url}/lineage',json=event_a_start_run)
except Exception as e:
    print(f'An Exception happend: {e}')
else:
    print(f'{get_now_formatted()}, response status code: {response.status_code}')

time.sleep(1)

event_a_complete_run = {
    "eventTime": get_now_formatted(),
    "producer": "https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client",
    "schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json#/definitions/RunEvent",

    "eventType": "COMPLETE",

    "run": {
        "runId": run_ids[0],
        "facets": {
            "nominalTime": {
                "_producer": "https://github.com/OpenLineage/OpenLineage/tree/0.10.0/integration/airflow",
                "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/NominalTimeRunFacet",
                "nominalStartTime": event_a_start_run['run']['facets']['nominalTime']['nominalStartTime'],
                "nominalEndTime": get_now_formatted(),
            }
        }
    },
    "job": {
        "namespace": namespaces[0],
        "name": job_names[0],
        "type": "SERVICE",
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
                "description": "Job A reads the source database.table and produces dataset_a."
            },
            "sourceCode": {
                "_producer": "https://some.producer.com/version/1.0",
                "_schemaURL": "https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/SourceCodeJobFacet.json",
                "language": "python",
                "sourceCode": """
                    my_df = spark.sql('SELECT upper(a), upper(b), upper(c), upper(d), upper(e) FROM database_abc.table_abc')
                    my_df.coalesce(1).write.parquet("dataset_a")
                """
            },
            "sourceCodeLocation": {
                "_producer": "https://some.producer.com/version/1.0",
                "_schemaURL": "https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/SourceCodeLocationJobFacet.json",
                "type": "git",
                "url": "https://github.com/lmassaoy/marquez-experiments/blob/main/experiments/lineage_across_diff_namespaces/experiment.py",
                "repoUrl": "https://github.com/lmassaoy/marquez-experiments.git",
                "path": "experiments/lineage_across_diff_namespaces/",
                "version": "https://github.com/lmassaoy/marquez-experiments/commit/0c79e8bfe4b406f80f096510b33ee42e466f0e28",
                "tag": "some_nice_tag",
                "branch": "main"
            },
            "sql": {
                "_producer": "https://some.producer.com/version/1.0",
                "_schemaURL": "https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/SQLJobFacet.json",
                "query": "SELECT upper(a), upper(b), upper(c), upper(d), upper(e) FROM database_abc.table_abc"
            }
        }
    },
    "outputs": [{
        "namespace": namespaces[0],
        "name": datasets[0],
        "physicalName": 'private.'+datasets[0],
        "type": "DATASET",
        "description": "This dataset is awesome!",
        "facets": {
            "schema": {
                "_producer": "https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client",
                "_schemaURL": "https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/spec/OpenLineage.json#/definitions/SchemaDatasetFacet",
                "fields": [
                    { "name": "a", "type": "VARCHAR", "description": "this is the column A"},
                    { "name": "b", "type": "INTEGER", "description": "this is the column B"},
                    { "name": "c", "type": "INTEGER", "description": "this is the column C"},
                    { "name": "d", "type": "DOUBLE", "description": "this is the column D"},
                    { "name": "e", "type": "TIMESTAMP", "description": "this is the column E"}
                ]
            },
            "columnLineage": {
                "_producer": "https://github.com/MarquezProject/marquez/blob/main/docker/metadata.json",
                "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/ColumnLineageDatasetFacet.json",
                "fields": {
                    "a": {
                        "inputFields": [
                            {
                                "namespace": namespaces[0],
                                "name": "table_abc",
                                "field": "a"
                            }
                        ],
                        "transformationDescription": "UPPER(a)",
                        "transformationType": "SQL"
                    },
                    "b": {
                        "inputFields": [
                            {
                                "namespace": namespaces[0],
                                "name": "table_abc",
                                "field": "a"
                            }
                        ],
                        "transformationDescription": "UPPER(b)",
                        "transformationType": "SQL"
                    },
                    "c": {
                        "inputFields": [
                            {
                                "namespace": namespaces[0],
                                "name": "table_abc",
                                "field": "a"
                            }
                        ],
                        "transformationDescription": "UPPER(c)",
                        "transformationType": "SQL"
                    },
                    "d": {
                        "inputFields": [
                            {
                                "namespace": namespaces[0],
                                "name": "table_abc",
                                "field": "a"
                            }
                        ],
                        "transformationDescription": "UPPER(d)",
                        "transformationType": "SQL"
                    },
                    "e": {
                        "inputFields": [
                            {
                                "namespace": namespaces[0],
                                "name": "table_abc",
                                "field": "a"
                            }
                        ],
                        "transformationDescription": "UPPER(e)",
                        "transformationType": "SQL"
                    }
                }
            },
            "additionalMetadata":{
                "_producer": "https://some.producer.com/version/1.0",
                "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/BaseFacet",
                "nurn": 'nurn:nu:data:dataset:'+datasets[0],
                "dataAssetType": "Delta Table",
                "description": "This dataset is awesome!",
            },
            "storage": {
                "_producer": "https://some.producer.com/version/1.0",
                "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/StorageDatasetFacet.json",
                "storageLayer": "delta",
                "fileFormat": "parquet",
                "location": {
                    "type": "s3",
                    "name": "itaipu_bucket",
                    "path": "/datasets/"+datasets[0]
                }
            }
        }
    }]
}

try:
    response = requests.post(url=f'{marquez_url}/lineage',json=event_a_complete_run)
except Exception as e:
    print(f'An Exception happend: {e}')
else:
    print(f'{get_now_formatted()}, response status code: {response.status_code}')


event_b_start_run = {
    "eventTime": get_now_formatted(),
    "producer": "https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client",
    "schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json#/definitions/RunEvent",

    "eventType": "START",

    "run": {
        "runId": run_ids[1],
        "facets": {
            "nominalTime": {
                "_producer": "https://github.com/OpenLineage/OpenLineage/tree/0.10.0/integration/airflow",
                "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/NominalTimeRunFacet",
                "nominalStartTime": get_now_formatted()
            },
            # "parentRun": {
            #     "_producer": "https://github.com/OpenLineage/OpenLineage/tree/0.10.0/integration/airflow",
            #     "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/ParentRunFacet",
            #     "job": {
            #         "namespace": namespaces[0],
            #         "name": job_names[0],
            #     },
            #     "run": {
            #         "runId": run_ids[0]
            #     }
            # }
        }
    },
    "job": {
        "namespace": namespaces[1],
        "name": job_names[1],
        "type": "SERVICE",
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
                "description": "Job B reads the source dataset_a and produces dataset_b."
            },
            "sourceCode": {
                "_producer": "https://some.producer.com/version/1.0",
                "_schemaURL": "https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/SourceCodeJobFacet.json",
                "language": "python",
                "sourceCode": """
                    my_df = spark.sql('SELECT a, d*2 as double_salary FROM dataset_a')
                    my_df.coalesce(1).write.parquet("dataset_b")
                """
            },
            "sourceCodeLocation": {
                "_producer": "https://some.producer.com/version/1.0",
                "_schemaURL": "https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/SourceCodeLocationJobFacet.json",
                "type": "git",
                "url": "https://github.com/lmassaoy/marquez-experiments/blob/main/experiments/lineage_across_diff_namespaces/experiment.py",
                "repoUrl": "https://github.com/lmassaoy/marquez-experiments.git",
                "path": "experiments/lineage_across_diff_namespaces/",
                "version": "https://github.com/lmassaoy/marquez-experiments/commit/0c79e8bfe4b406f80f096510b33ee42e466f0e28",
                "tag": "some_nice_tag",
                "branch": "main"
            },
            "sql": {
                "_producer": "https://some.producer.com/version/1.0",
                "_schemaURL": "https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/SQLJobFacet.json",
                "query": "SELECT a, d*2 as double_salary FROM dataset_a"
            }
        }
    },
    "inputs": [{
        "namespace": namespaces[0],
        "name": datasets[0]
    }]
}

try:
    response = requests.post(url=f'{marquez_url}/lineage',json=event_b_start_run)
except Exception as e:
    print(f'An Exception happend: {e}')
else:
    print(f'{get_now_formatted()}, response status code: {response.status_code}')

time.sleep(1)

event_b_complete_run = {
    "eventTime": get_now_formatted(),
    "producer": "https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client",
    "schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json#/definitions/RunEvent",

    "eventType": "COMPLETE",

    "run": {
        "runId": run_ids[1],
        "facets": {
            "nominalTime": {
                "_producer": "https://github.com/OpenLineage/OpenLineage/tree/0.10.0/integration/airflow",
                "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/NominalTimeRunFacet",
                "nominalStartTime": event_b_start_run['run']['facets']['nominalTime']['nominalStartTime'],
                "nominalEndTime": get_now_formatted()
            },
            # "parentRun": {
            #     "_producer": "https://github.com/OpenLineage/OpenLineage/tree/0.10.0/integration/airflow",
            #     "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/ParentRunFacet",
            #     "job": {
            #         "namespace": namespaces[0],
            #         "name": job_names[0],
            #     },
            #     "run": {
            #         "runId": run_ids[0]
            #     }
            # }
        }
    },
    "job": {
        "namespace": namespaces[1],
        "name": job_names[1],
        "type": "SERVICE",
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
                "description": "Job B reads the source dataset_a and produces dataset_b."
            },
            "sourceCode": {
                "_producer": "https://some.producer.com/version/1.0",
                "_schemaURL": "https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/SourceCodeJobFacet.json",
                "language": "python",
                "sourceCode": """
                    my_df = spark.sql('SELECT a, d*2 as double_salary FROM dataset_a')
                    my_df.coalesce(1).write.parquet("dataset_b")
                """
            },
            "sourceCodeLocation": {
                "_producer": "https://some.producer.com/version/1.0",
                "_schemaURL": "https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/SourceCodeLocationJobFacet.json",
                "type": "git",
                "url": "https://github.com/lmassaoy/marquez-experiments/blob/main/experiments/lineage_across_diff_namespaces/experiment.py",
                "repoUrl": "https://github.com/lmassaoy/marquez-experiments.git",
                "path": "experiments/lineage_across_diff_namespaces/",
                "version": "https://github.com/lmassaoy/marquez-experiments/commit/0c79e8bfe4b406f80f096510b33ee42e466f0e28",
                "tag": "some_nice_tag",
                "branch": "main"
            },
            "sql": {
                "_producer": "https://some.producer.com/version/1.0",
                "_schemaURL": "https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/SQLJobFacet.json",
                "query": "SELECT a, d*2 as double_salary FROM dataset_a"
            }
        }
    },
    "outputs": [{
        "namespace": namespaces[1],
        "name": datasets[1],
        "physicalName": 'private.'+datasets[1],
        "type": "DATASET",
        "description": "This dataset is awesome!",
        "facets": {
            "schema": {
                "_producer": "https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client",
                "_schemaURL": "https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/spec/OpenLineage.json#/definitions/SchemaDatasetFacet",
                "fields": [
                    { "name": "a", "type": "VARCHAR", "description": "this is the column A"},
                    { "name": "double_salary", "type": "INTEGER", "description": "this is the column DOUBLE SALARY"}
                ]
            },
            "columnLineage": {
                "_producer": "https://github.com/MarquezProject/marquez/blob/main/docker/metadata.json",
                "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/ColumnLineageDatasetFacet.json",
                "fields": {
                    "a": {
                        "inputFields": [
                            {
                                "namespace": namespaces[0],
                                "name": datasets[0],
                                "field": "a"
                            }
                        ],
                        "transformationDescription": "",
                        "transformationType": "SQL"
                    },
                    "double_salary": {
                        "inputFields": [
                            {
                                "namespace": namespaces[0],
                                "name": datasets[0],
                                "field": "d"
                            }
                        ],
                        "transformationDescription": "d*2",
                        "transformationType": "SQL"
                    }
                }
            },
            "additionalMetadata":{
                "_producer": "https://some.producer.com/version/1.0",
                "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/BaseFacet",
                "nurn": 'nurn:nu:data:dataset:'+datasets[1],
                "dataAssetType": "Delta Table",
                "description": "This dataset is awesome!",
            },
            "storage": {
                "_producer": "https://some.producer.com/version/1.0",
                "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/StorageDatasetFacet.json",
                "storageLayer": "delta",
                "fileFormat": "parquet",
                "location": {
                    "type": "s3",
                    "name": "itaipu_bucket",
                    "path": "/datasets/"+datasets[1]
                }
            }
        }
    }]
}

try:
    response = requests.post(url=f'{marquez_url}/lineage',json=event_b_complete_run)
except Exception as e:
    print(f'An Exception happend: {e}')
else:
    print(f'{get_now_formatted()}, response status code: {response.status_code}')


event_c_start_run = {
    "eventTime": get_now_formatted(),
    "producer": "https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client",
    "schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json#/definitions/RunEvent",

    "eventType": "START",

    "run": {
        "runId": run_ids[2],
        "facets": {
            "nominalTime": {
                "_producer": "https://github.com/OpenLineage/OpenLineage/tree/0.10.0/integration/airflow",
                "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/NominalTimeRunFacet",
                "nominalStartTime": get_now_formatted()
            },
            # "parentRun": {
            #     "_producer": "https://github.com/OpenLineage/OpenLineage/tree/0.10.0/integration/airflow",
            #     "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/ParentRunFacet",
            #     "job": {
            #         "namespace": namespaces[0],
            #         "name": job_names[0],
            #     },
            #     "run": {
            #         "runId": run_ids[0]
            #     }
            # }
        }
    },
    "job": {
        "namespace": namespaces[2],
        "name": job_names[2],
        "type": "SERVICE",
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
                "description": "Job C reads the source dataset_a + dataset_b and produces dataset_c."
            },
            "sourceCode": {
                "_producer": "https://some.producer.com/version/1.0",
                "_schemaURL": "https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/SourceCodeJobFacet.json",
                "language": "python",
                "sourceCode": """
                    my_c_query = '''
                        SELECT
                            dsa.a as event_id,
                            dsa.e as event_time,
                            dsa.b + dsa.c as sum_bc,
                            dsa.d as salary,
                            dsb.double_salary as double_salary
                        FROM
                            dataset_a dsa
                        LEFT JOIN
                            dataset_b dsb
                        ON
                            dsa.a = dsb.a
                    '''

                    my_df = spark.sql('')
                    my_df.coalesce(1).write.parquet("dataset_c")
                """
            },
            "sourceCodeLocation": {
                "_producer": "https://some.producer.com/version/1.0",
                "_schemaURL": "https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/SourceCodeLocationJobFacet.json",
                "type": "git",
                "url": "https://github.com/lmassaoy/marquez-experiments/blob/main/experiments/lineage_across_diff_namespaces/experiment.py",
                "repoUrl": "https://github.com/lmassaoy/marquez-experiments.git",
                "path": "experiments/lineage_across_diff_namespaces/",
                "version": "https://github.com/lmassaoy/marquez-experiments/commit/0c79e8bfe4b406f80f096510b33ee42e466f0e28",
                "tag": "some_nice_tag",
                "branch": "main"
            },
            "sql": {
                "_producer": "https://some.producer.com/version/1.0",
                "_schemaURL": "https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/SQLJobFacet.json",
                "query": """
SELECT
    dsa.a as event_id,
    dsa.e as event_time,
    dsa.b + dsa.c as sum_bc,
    dsa.d as salary,
    dsb.double_salary as double_salary
FROM
    dataset_a dsa
LEFT JOIN
    dataset_b dsb
ON
    dsa.a = dsb.a
                """
            }
        }
    },
    "inputs": [
        {
            "namespace": namespaces[0],
            "name": datasets[0]
        },
        {
            "namespace": namespaces[1],
            "name": datasets[1]
        }
    ]
}

try:
    response = requests.post(url=f'{marquez_url}/lineage',json=event_c_start_run)
except Exception as e:
    print(f'An Exception happend: {e}')
else:
    print(f'{get_now_formatted()}, response status code: {response.status_code}')

time.sleep(1)

event_c_complete_run = {
    "eventTime": get_now_formatted(),
    "producer": "https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client",
    "schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json#/definitions/RunEvent",

    "eventType": "COMPLETE",

    "run": {
        "runId": run_ids[2],
        "facets": {
            "nominalTime": {
                "_producer": "https://github.com/OpenLineage/OpenLineage/tree/0.10.0/integration/airflow",
                "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/NominalTimeRunFacet",
                "nominalStartTime": event_c_start_run['run']['facets']['nominalTime']['nominalStartTime'],
                "nominalEndTime": get_now_formatted()
            },
            # "parentRun": {
            #     "_producer": "https://github.com/OpenLineage/OpenLineage/tree/0.10.0/integration/airflow",
            #     "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/ParentRunFacet",
            #     "job": {
            #         "namespace": namespaces[0],
            #         "name": job_names[0],
            #     },
            #     "run": {
            #         "runId": run_ids[0]
            #     }
            # }
        }
    },
    "job": {
        "namespace": namespaces[2],
        "name": job_names[2],
        "type": "SERVICE",
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
                "description": "Job C reads the source dataset_a + dataset_b and produces dataset_c."
            },
            "sourceCode": {
                "_producer": "https://some.producer.com/version/1.0",
                "_schemaURL": "https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/SourceCodeJobFacet.json",
                "language": "python",
                "sourceCode": """
                    my_c_query = '''
                        SELECT
                            dsa.a as event_id,
                            dsa.e as event_time,
                            dsa.b + dsa.c as sum_bc,
                            dsa.d as salary,
                            dsb.double_salary as double_salary
                        FROM
                            dataset_a dsa
                        LEFT JOIN
                            dataset_b dsb
                        ON
                            dsa.a = dsb.a
                    '''

                    my_df = spark.sql('')
                    my_df.coalesce(1).write.parquet("dataset_c")
                """
            },
            "sourceCodeLocation": {
                "_producer": "https://some.producer.com/version/1.0",
                "_schemaURL": "https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/SourceCodeLocationJobFacet.json",
                "type": "git",
                "url": "https://github.com/lmassaoy/marquez-experiments/blob/main/experiments/lineage_across_diff_namespaces/experiment.py",
                "repoUrl": "https://github.com/lmassaoy/marquez-experiments.git",
                "path": "experiments/lineage_across_diff_namespaces/",
                "version": "https://github.com/lmassaoy/marquez-experiments/commit/0c79e8bfe4b406f80f096510b33ee42e466f0e28",
                "tag": "some_nice_tag",
                "branch": "main"
            },
            "sql": {
                "_producer": "https://some.producer.com/version/1.0",
                "_schemaURL": "https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/SQLJobFacet.json",
                "query": """
SELECT
    dsa.a as event_id,
    dsa.e as event_time,
    dsa.b + dsa.c as sum_bc,
    dsa.d as salary,
    dsb.double_salary as double_salary
FROM
    dataset_a dsa
LEFT JOIN
    dataset_b dsb
ON
    dsa.a = dsb.a
                """
            }
        }
    },
    "outputs": [{
        "namespace": namespaces[2],
        "name": datasets[2],
        "physicalName": 'private.'+datasets[2],
        "type": "DATASET",
        "description": "This dataset is awesome!",
        "facets": {
            "schema": {
                "_producer": "https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client",
                "_schemaURL": "https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/spec/OpenLineage.json#/definitions/SchemaDatasetFacet",
                "fields": [
                    { "name": "event_id", "type": "VARCHAR", "description": "this is the column EVENT ID"},
                    { "name": "event_time", "type": "TIMESTAMP", "description": "this is the column EVENT TIME"},
                    { "name": "sum_bc", "type": "INTEGER", "description": "this is the column SUM BC"},
                    { "name": "salary", "type": "VARCHAR", "description": "this is the column SALARY"},
                    { "name": "double_salary", "type": "INTEGER", "description": "this is the column DOUBLE SALARY"}
                ]
            },
            "columnLineage": {
                "_producer": "https://github.com/MarquezProject/marquez/blob/main/docker/metadata.json",
                "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/ColumnLineageDatasetFacet.json",
                "fields": {
                    "event_id": {
                        "inputFields": [
                            {
                                "namespace": namespaces[0],
                                "name": datasets[0],
                                "field": "a"
                            }
                        ],
                        "transformationDescription": "",
                        "transformationType": "SQL"
                    },
                    "event_time": {
                        "inputFields": [
                            {
                                "namespace": namespaces[0],
                                "name": datasets[0],
                                "field": "e"
                            }
                        ],
                        "transformationDescription": "",
                        "transformationType": "SQL"
                    },
                    "sum_bc": {
                        "inputFields": [
                            {
                                "namespace": namespaces[0],
                                "name": datasets[0],
                                "field": "b"
                            },
                            {
                                "namespace": namespaces[0],
                                "name": datasets[0],
                                "field": "c"
                            }
                        ],
                        "transformationDescription": "b+c",
                        "transformationType": "SQL"
                    },
                    "salary": {
                        "inputFields": [
                            {
                                "namespace": namespaces[0],
                                "name": datasets[0],
                                "field": "d"
                            }
                        ],
                        "transformationDescription": "",
                        "transformationType": "SQL"
                    },
                    "double_salary": {
                        "inputFields": [
                            {
                                "namespace": namespaces[1],
                                "name": datasets[1],
                                "field": "double_salary"
                            }
                        ],
                        "transformationDescription": "",
                        "transformationType": "SQL"
                    }
                }
            },
            "additionalMetadata":{
                "_producer": "https://some.producer.com/version/1.0",
                "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/BaseFacet",
                "nurn": 'nurn:nu:data:dataset:'+datasets[2],
                "dataAssetType": "Delta Table",
                "description": "This dataset is awesome!",
            },
            "storage": {
                "_producer": "https://some.producer.com/version/1.0",
                "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/StorageDatasetFacet.json",
                "storageLayer": "delta",
                "fileFormat": "parquet",
                "location": {
                    "type": "s3",
                    "name": "itaipu_bucket",
                    "path": "/datasets/"+datasets[2]
                }
            }
        }
    }]
}

try:
    response = requests.post(url=f'{marquez_url}/lineage',json=event_c_complete_run)
except Exception as e:
    print(f'An Exception happend: {e}')
else:
    print(f'{get_now_formatted()}, response status code: {response.status_code}')

########

event_d_start_run = {
    "eventTime": get_now_formatted(),
    "producer": "https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client",
    "schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json#/definitions/RunEvent",

    "eventType": "START",

    "run": {
        "runId": run_ids[3],
        "facets": {
            "nominalTime": {
                "_producer": "https://github.com/OpenLineage/OpenLineage/tree/0.10.0/integration/airflow",
                "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/NominalTimeRunFacet",
                "nominalStartTime": get_now_formatted()
            },
        }
    },
    "job": {
        "namespace": namespaces[3],
        "name": job_names[3],
        "type": "SERVICE",
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
                "description": "Job D reads the source dataset_c and produces dataset_d."
            },
            "sourceCode": {
                "_producer": "https://some.producer.com/version/1.0",
                "_schemaURL": "https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/SourceCodeJobFacet.json",
                "language": "python",
                "sourceCode": """
                    my_df = spark.sql('SELECT count(event_id) as qty_events, avg(salary) as avg_salary FROM dataset_c')
                    my_df.coalesce(1).write.parquet("dataset_d")
                """
            },
            "sourceCodeLocation": {
                "_producer": "https://some.producer.com/version/1.0",
                "_schemaURL": "https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/SourceCodeLocationJobFacet.json",
                "type": "git",
                "url": "https://github.com/lmassaoy/marquez-experiments/blob/main/experiments/lineage_across_diff_namespaces/experiment.py",
                "repoUrl": "https://github.com/lmassaoy/marquez-experiments.git",
                "path": "experiments/lineage_across_diff_namespaces/",
                "version": "https://github.com/lmassaoy/marquez-experiments/commit/0c79e8bfe4b406f80f096510b33ee42e466f0e28",
                "tag": "some_nice_tag",
                "branch": "main"
            },
            "sql": {
                "_producer": "https://some.producer.com/version/1.0",
                "_schemaURL": "https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/SQLJobFacet.json",
                "query": "SELECT count(event_id) as qty_events, avg(salary) as avg_salary FROM dataset_c"
            }
        }
    },
    "inputs": [{
        "namespace": namespaces[2],
        "name": datasets[2]
    }]
}

try:
    response = requests.post(url=f'{marquez_url}/lineage',json=event_d_start_run)
except Exception as e:
    print(f'An Exception happend: {e}')
else:
    print(f'{get_now_formatted()}, response status code: {response.status_code}')

time.sleep(1)

event_d_complete_run = {
    "eventTime": get_now_formatted(),
    "producer": "https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client",
    "schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json#/definitions/RunEvent",

    "eventType": "COMPLETE",

    "run": {
        "runId": run_ids[3],
        "facets": {
            "nominalTime": {
                "_producer": "https://github.com/OpenLineage/OpenLineage/tree/0.10.0/integration/airflow",
                "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/NominalTimeRunFacet",
                "nominalStartTime": event_d_start_run['run']['facets']['nominalTime']['nominalStartTime'],
                "nominalEndTime": get_now_formatted()
            },
        }
    },
    "job": {
        "namespace": namespaces[3],
        "name": job_names[3],
        "type": "SERVICE",
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
                "description": "Job D reads the source dataset_c and produces dataset_d."
            },
            "sourceCode": {
                "_producer": "https://some.producer.com/version/1.0",
                "_schemaURL": "https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/SourceCodeJobFacet.json",
                "language": "python",
                "sourceCode": """
                    my_df = spark.sql('SELECT count(event_id) as qty_events, avg(salary) as avg_salary FROM dataset_c')
                    my_df.coalesce(1).write.parquet("dataset_d")
                """
            },
            "sourceCodeLocation": {
                "_producer": "https://some.producer.com/version/1.0",
                "_schemaURL": "https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/SourceCodeLocationJobFacet.json",
                "type": "git",
                "url": "https://github.com/lmassaoy/marquez-experiments/blob/main/experiments/lineage_across_diff_namespaces/experiment.py",
                "repoUrl": "https://github.com/lmassaoy/marquez-experiments.git",
                "path": "experiments/lineage_across_diff_namespaces/",
                "version": "https://github.com/lmassaoy/marquez-experiments/commit/0c79e8bfe4b406f80f096510b33ee42e466f0e28",
                "tag": "some_nice_tag",
                "branch": "main"
            },
            "sql": {
                "_producer": "https://some.producer.com/version/1.0",
                "_schemaURL": "https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/SQLJobFacet.json",
                "query": "SELECT count(event_id) as qty_events, avg(salary) as avg_salary FROM dataset_c"
            }
        }
    },
    "outputs": [{
        "namespace": namespaces[3],
        "name": datasets[3],
        "physicalName": 'private.'+datasets[3],
        "type": "DATASET",
        "description": "This dataset is awesome!",
        "facets": {
            "schema": {
                "_producer": "https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client",
                "_schemaURL": "https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/spec/OpenLineage.json#/definitions/SchemaDatasetFacet",
                "fields": [
                    { "name": "qty_events", "type": "VARCHAR", "description": "this is the column Quantity of Events"},
                    { "name": "avg_salary", "type": "INTEGER", "description": "this is the column Average Salary"}
                ]
            },
            "columnLineage": {
                "_producer": "https://github.com/MarquezProject/marquez/blob/main/docker/metadata.json",
                "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/ColumnLineageDatasetFacet.json",
                "fields": {
                    "qty_events": {
                        "inputFields": [
                            {
                                "namespace": namespaces[2],
                                "name": datasets[2],
                                "field": "event_id"
                            }
                        ],
                        "transformationDescription": "count(event_id)",
                        "transformationType": "SQL"
                    },
                    "avg_salary": {
                        "inputFields": [
                            {
                                "namespace": namespaces[2],
                                "name": datasets[2],
                                "field": "salary"
                            }
                        ],
                        "transformationDescription": "avg(salary)",
                        "transformationType": "SQL"
                    }
                }
            },
            "additionalMetadata":{
                "_producer": "https://some.producer.com/version/1.0",
                "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/BaseFacet",
                "nurn": 'nurn:nu:data:dataset:'+datasets[3],
                "dataAssetType": "Delta Table",
                "description": "This dataset is awesome!",
            },
            "storage": {
                "_producer": "https://some.producer.com/version/1.0",
                "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/StorageDatasetFacet.json",
                "storageLayer": "delta",
                "fileFormat": "parquet",
                "location": {
                    "type": "s3",
                    "name": "itaipu_bucket",
                    "path": "/datasets/"+datasets[3]
                }
            }
        }
    }]
}

try:
    response = requests.post(url=f'{marquez_url}/lineage',json=event_d_complete_run)
except Exception as e:
    print(f'An Exception happend: {e}')
else:
    print(f'{get_now_formatted()}, response status code: {response.status_code}')

#####

event_e_start_run = {
    "eventTime": get_now_formatted(),
    "producer": "https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client",
    "schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json#/definitions/RunEvent",

    "eventType": "START",

    "run": {
        "runId": run_ids[4],
        "facets": {
            "nominalTime": {
                "_producer": "https://github.com/OpenLineage/OpenLineage/tree/0.10.0/integration/airflow",
                "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/NominalTimeRunFacet",
                "nominalStartTime": get_now_formatted()
            },
        }
    },
    "job": {
        "namespace": namespaces[4],
        "name": job_names[4],
        "type": "SERVICE",
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
                "description": "Job E reads the source dataset_c and produces dataset_d."
            },
            "sourceCode": {
                "_producer": "https://some.producer.com/version/1.0",
                "_schemaURL": "https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/SourceCodeJobFacet.json",
                "language": "python",
                "sourceCode": """
                    my_df = spark.sql('SELECT count(event_id) as qty_events, avg(salary) as avg_salary FROM dataset_c')
                    my_df.coalesce(1).write.parquet("dataset_e")
                """
            },
            "sourceCodeLocation": {
                "_producer": "https://some.producer.com/version/1.0",
                "_schemaURL": "https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/SourceCodeLocationJobFacet.json",
                "type": "git",
                "url": "https://github.com/lmassaoy/marquez-experiments/blob/main/experiments/lineage_across_diff_namespaces/experiment.py",
                "repoUrl": "https://github.com/lmassaoy/marquez-experiments.git",
                "path": "experiments/lineage_across_diff_namespaces/",
                "version": "https://github.com/lmassaoy/marquez-experiments/commit/0c79e8bfe4b406f80f096510b33ee42e466f0e28",
                "tag": "some_nice_tag",
                "branch": "main"
            },
            "sql": {
                "_producer": "https://some.producer.com/version/1.0",
                "_schemaURL": "https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/SQLJobFacet.json",
                "query": "SELECT count(event_id) as qty_events, avg(salary) as avg_salary FROM dataset_c"
            }
        }
    },
    "inputs": [{
        "namespace": namespaces[2],
        "name": datasets[2]
    }]
}

try:
    response = requests.post(url=f'{marquez_url}/lineage',json=event_e_start_run)
except Exception as e:
    print(f'An Exception happend: {e}')
else:
    print(f'{get_now_formatted()}, response status code: {response.status_code}')

time.sleep(1)

event_e_complete_run = {
    "eventTime": get_now_formatted(),
    "producer": "https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client",
    "schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json#/definitions/RunEvent",

    "eventType": "COMPLETE",

    "run": {
        "runId": run_ids[4],
        "facets": {
            "nominalTime": {
                "_producer": "https://github.com/OpenLineage/OpenLineage/tree/0.10.0/integration/airflow",
                "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/NominalTimeRunFacet",
                "nominalStartTime": event_e_start_run['run']['facets']['nominalTime']['nominalStartTime'],
                "nominalEndTime": get_now_formatted()
            },
        }
    },
    "job": {
        "namespace": namespaces[4],
        "name": job_names[4],
        "type": "SERVICE",
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
                "description": "Job E reads the source dataset_c and produces dataset_e."
            },
            "sourceCode": {
                "_producer": "https://some.producer.com/version/1.0",
                "_schemaURL": "https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/SourceCodeJobFacet.json",
                "language": "python",
                "sourceCode": """
                    my_df = spark.sql('SELECT count(event_id) as qty_events, avg(salary) as avg_salary FROM dataset_c')
                    my_df.coalesce(1).write.parquet("dataset_e")
                """
            },
            "sourceCodeLocation": {
                "_producer": "https://some.producer.com/version/1.0",
                "_schemaURL": "https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/SourceCodeLocationJobFacet.json",
                "type": "git",
                "url": "https://github.com/lmassaoy/marquez-experiments/blob/main/experiments/lineage_across_diff_namespaces/experiment.py",
                "repoUrl": "https://github.com/lmassaoy/marquez-experiments.git",
                "path": "experiments/lineage_across_diff_namespaces/",
                "version": "https://github.com/lmassaoy/marquez-experiments/commit/0c79e8bfe4b406f80f096510b33ee42e466f0e28",
                "tag": "some_nice_tag",
                "branch": "main"
            },
            "sql": {
                "_producer": "https://some.producer.com/version/1.0",
                "_schemaURL": "https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/SQLJobFacet.json",
                "query": "SELECT count(event_id) as qty_events, avg(salary) as avg_salary FROM dataset_c"
            }
        }
    },
    "outputs": [{
        "namespace": namespaces[4],
        "name": datasets[4],
        "physicalName": 'private.'+datasets[4],
        "type": "DATASET",
        "description": "This dataset is awesome!",
        "facets": {
            "schema": {
                "_producer": "https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client",
                "_schemaURL": "https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/spec/OpenLineage.json#/definitions/SchemaDatasetFacet",
                "fields": [
                    { "name": "qty_events", "type": "VARCHAR", "description": "this is the column Quantity of Events"},
                    { "name": "avg_salary", "type": "INTEGER", "description": "this is the column Average Salary"}
                ]
            },
            "columnLineage": {
                "_producer": "https://github.com/MarquezProject/marquez/blob/main/docker/metadata.json",
                "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/ColumnLineageDatasetFacet.json",
                "fields": {
                    "qty_events": {
                        "inputFields": [
                            {
                                "namespace": namespaces[2],
                                "name": datasets[2],
                                "field": "event_id"
                            }
                        ],
                        "transformationDescription": "count(event_id)",
                        "transformationType": "SQL"
                    },
                    "avg_salary": {
                        "inputFields": [
                            {
                                "namespace": namespaces[2],
                                "name": datasets[2],
                                "field": "salary"
                            }
                        ],
                        "transformationDescription": "avg(salary)",
                        "transformationType": "SQL"
                    }
                }
            },
            "additionalMetadata":{
                "_producer": "https://some.producer.com/version/1.0",
                "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/BaseFacet",
                "nurn": 'nurn:nu:data:dataset:'+datasets[4],
                "dataAssetType": "Delta Table",
                "description": "This dataset is awesome!",
            },
            "storage": {
                "_producer": "https://some.producer.com/version/1.0",
                "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/StorageDatasetFacet.json",
                "storageLayer": "delta",
                "fileFormat": "parquet",
                "location": {
                    "type": "s3",
                    "name": "itaipu_bucket",
                    "path": "/datasets/"+datasets[4]
                }
            }
        }
    }]
}

try:
    response = requests.post(url=f'{marquez_url}/lineage',json=event_e_complete_run)
except Exception as e:
    print(f'An Exception happend: {e}')
else:
    print(f'{get_now_formatted()}, response status code: {response.status_code}')