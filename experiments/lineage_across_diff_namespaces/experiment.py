import time
import requests
import uuid


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
    "eventTime": "2023-10-10T19:52:00.001+10:00",
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
                "description": "Job A reads the source database.table and produce a parquet table."
            },
            "sourceCode": {
                "_producer": "https://some.producer.com/version/1.0",
                "_schemaURL": "https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/SourceCodeJobFacet.json",
                "language": "python",
                "sourceCode": """
                    spark.sql('SELECT a, b, c, d, e FROM database_abc.table_abc')
                """
            },
            "sourceCodeLocation": {
                "_producer": "https://some.producer.com/version/1.0",
                "_schemaURL": "https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/SourceCodeLocationJobFacet.json",
                "type": "git",
                "url": "https://github.com/MarquezProject/marquez-airflow-quickstart/blob/693e35482bc2e526ced2b5f9f76ef83dec6ec691/dags/hello.py",
                "repoUrl": "https://github.com/lmassaoy/marquez-experiments.git",
                "path": "path/to/my/dags",
                "version": "git: the git sha | Svn: the revision number",
                "tag": "example",
                "branch": "main"
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
    print(response.status_code)

time.sleep(5)

event_a_complete_run = {
    "eventTime": "2023-10-10T20:52:00.001+10:00",
    "producer": "https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client",
    "schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json#/definitions/RunEvent",

    "eventType": "COMPLETE",

    "run": {
        "runId": "5990c811-f0ce-42a3-88ed-dd1714bf4562"
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
                "description": "Job A reads the source database.table and produce a parquet table."
            },
            "sourceCode": {
                "_producer": "https://some.producer.com/version/1.0",
                "_schemaURL": "https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/SourceCodeJobFacet.json",
                "language": "python",
                "sourceCode": """
                    spark.sql('SELECT a, b, c, d, e FROM database_abc.table_abc')
                """
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
    print(response.status_code)