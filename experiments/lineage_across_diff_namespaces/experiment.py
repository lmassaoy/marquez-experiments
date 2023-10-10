import time
import requests
import uuid
from datetime import datetime
import pytz


def get_now_formatted():
    current_timestamp = datetime.now(tz=pytz.timezone("America/Sao_Paulo")).strftime("%Y-%m-%dT%H:%M:%S.%f%z")
    return current_timestamp[:-8]+current_timestamp[-5:-2]+":"+current_timestamp[-2:]


print(f'starting at: {get_now_formatted()}')
marquez_url = "http://localhost:5000/api/v1"


run_ids = [ str(uuid.uuid4()) for i in range(3) ]

namespaces = [
    "namespace_1", "namespace_2", "namespace_3"
]
job_names = [
    "job_a", "job_b", "job_c"
]
datasets = [
   "dataset_a", "dataset_b", "dataset_c"
]

event_a_start_run = {
    "eventTime": get_now_formatted(),
    "producer": "https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client",
    "schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json#/definitions/RunEvent",

    "eventType": "START",

    "run": {
        "runId": run_ids[0]
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

time.sleep(5)

event_a_complete_run = {
    "eventTime": get_now_formatted(),
    "producer": "https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client",
    "schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json#/definitions/RunEvent",

    "eventType": "COMPLETE",

    "run": {
        "runId": run_ids[0]
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
        "physicalName": 's3://my_bucket/'+datasets[0],
        "type": "FILE",
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
        "runId": run_ids[1]
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

time.sleep(5)

event_b_complete_run = {
    "eventTime": get_now_formatted(),
    "producer": "https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client",
    "schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json#/definitions/RunEvent",

    "eventType": "COMPLETE",

    "run": {
        "runId": run_ids[1]
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
        "physicalName": 's3://my_bucket/'+datasets[1],
        "type": "FILE",
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