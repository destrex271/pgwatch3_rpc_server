import yaml
import os
import pytest
from testcontainers.postgres import PostgresContainer
from pgwatch_pb2 import MeasurementEnvelope, Reply

@pytest.fixture(scope="module", autouse=True)
def setup_catalog(tmp_path_factory):
    with PostgresContainer("postgres:16") as postgres:
        tmp_path = tmp_path_factory.getbasetemp()
        os.environ["PYICEBERG_HOME"] = tmp_path.as_posix()

        test_iceberg_yaml = {
            "catalog": {
                "pgcatalog": {
                    "uri": postgres.get_connection_url(),
                    "type": "sql",
                    "init_catalog_tables": True
                }
            }
        }

        test_pyiceberg_file = tmp_path / ".pyiceberg.yaml"
        with open(test_pyiceberg_file.as_posix(), 'w') as file:
            yaml.dump(test_iceberg_yaml, file, default_flow_style=False, allow_unicode=True)

        yield 


def test_IcebergReceiver(tmp_path):
    # Late import to allow `PYICEERG_HOME` env to be
    # set by `setup_catalog` fixture before 
    # pyiceberg reads it on init
    from iceberg_receiver import IcebergReceiver

    recv = IcebergReceiver(tmp_path.as_posix()) 
    msg = MeasurementEnvelope(DBName="test",MetricName="test")
    reply = recv.UpdateMeasurements(msg, None)
    assert reply == Reply(logmsg="Metrics Inserted in iceberg.")

    paTable = recv.tbl.scan().to_arrow()
    pyList = paTable.to_pylist()

    assert pyList[0]["DBName"] == "test"
    assert pyList[0]["MetricName"] == "test"
