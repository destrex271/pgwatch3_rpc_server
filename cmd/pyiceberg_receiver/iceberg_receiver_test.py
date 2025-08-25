import yaml
import os
import pytest
import shutil
from testcontainers.postgres import PostgresContainer
from iceberg_receiver import IcebergReceiver
from pgwatch_pb2 import MeasurementEnvelope, Reply

@pytest.fixture(scope="function", autouse=True)
def setup_postgres_catalog():
    with PostgresContainer("postgres:16") as postgres:
        os.rename(".pyiceberg.yaml", ".pyiceberg2.yaml")
        iceberg_yaml = {
            "catalog": {
                "pgcatalog": {
                    "uri": postgres.get_connection_url(),
                    "type": "sql",
                    "init_catalog_tables": True
                }
            }
        }
        with open('.pyiceberg.yaml', 'w') as file:
            yaml.dump(iceberg_yaml, file, default_flow_style=False, allow_unicode=True)

        yield 

        os.rename(".pyiceberg2.yaml", ".pyiceberg.yaml")
        shutil.rmtree("./data/")


def test_IcebergReceiver():
    recv = IcebergReceiver("./data/") 
    msg = MeasurementEnvelope(DBName="test",MetricName="test")
    reply = recv.UpdateMeasurements(msg, None)
    assert reply == Reply(logmsg="Metrics Inserted in iceberg.")

    paTable = recv.tbl.scan().to_arrow()
    pyList = paTable.to_pylist()

    assert pyList[0]["DBName"] == "test"
    assert pyList[0]["MetricName"] == "test"

    recv.catalog.drop_table("pgwatch.metrics")