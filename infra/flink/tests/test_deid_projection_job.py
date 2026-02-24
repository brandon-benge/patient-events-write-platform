import json
import sys
import types
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
INFRA_ROOT = REPO_ROOT / "infra"
if str(INFRA_ROOT) not in sys.path:
    sys.path.insert(0, str(INFRA_ROOT))


def _install_pyflink_stubs() -> None:
    if "pyflink" in sys.modules:
        return

    if "psycopg2" not in sys.modules:
        psycopg2 = types.ModuleType("psycopg2")

        class _FakeConnection:
            def __init__(self, *args, **kwargs):
                self.autocommit = False

            def cursor(self):
                class _FakeCursor:
                    rowcount = 0

                    def execute(self, *args, **kwargs):
                        return None

                    def close(self):
                        return None

                return _FakeCursor()

            def commit(self):
                return None

            def rollback(self):
                return None

            def close(self):
                return None

        def _connect(*args, **kwargs):
            return _FakeConnection()

        psycopg2.connect = _connect
        sys.modules["psycopg2"] = psycopg2

    # Stub py4j module
    py4j = types.ModuleType("py4j")
    py4j_gateway = types.ModuleType("py4j.java_gateway")
    
    def _java_import(*args, **kwargs):
        pass
    
    py4j_gateway.java_import = _java_import
    sys.modules["py4j"] = py4j
    sys.modules["py4j.java_gateway"] = py4j_gateway

    pyflink = types.ModuleType("pyflink")
    datastream = types.ModuleType("pyflink.datastream")
    ds_functions = types.ModuleType("pyflink.datastream.functions")
    ds_kafka = types.ModuleType("pyflink.datastream.connectors.kafka")
    ds_jdbc = types.ModuleType("pyflink.datastream.connectors.jdbc")
    common = types.ModuleType("pyflink.common")
    common_serialization = types.ModuleType("pyflink.common.serialization")
    common_typeinfo = types.ModuleType("pyflink.common.typeinfo")

    class _FlatMapFunction:
        pass

    class _MapFunction:
        pass

    class _RichSinkFunction:
        pass

    class _RichFlatMapFunction:
        pass

    class _RuntimeContext:
        pass

    class _StreamExecutionEnvironment:
        @staticmethod
        def get_execution_environment():
            return _StreamExecutionEnvironment()

        def set_parallelism(self, _p):
            return None

    class _FlinkKafkaConsumer:
        def __init__(self, *args, **kwargs):
            pass

    class _FlinkKafkaProducer:
        def __init__(self, *args, **kwargs):
            pass

    class _JdbcSink:
        @staticmethod
        def sink(*args, **kwargs):
            return object()

    class _JdbcConnectionOptions:
        class JdbcConnectionOptionsBuilder:
            def with_url(self, *_):
                return self

            def with_driver_name(self, *_):
                return self

            def with_user_name(self, *_):
                return self

            def with_password(self, *_):
                return self

            def build(self):
                return object()

    class _JdbcExecutionOptions:
        @staticmethod
        def builder():
            class _Builder:
                def with_batch_size(self, *_):
                    return self

                def with_batch_interval_ms(self, *_):
                    return self

                def with_max_retries(self, *_):
                    return self

                def build(self):
                    return object()

            return _Builder()

    class _SimpleStringSchema:
        pass

    class _Types:
        @staticmethod
        def ROW(_):
            return object()

        @staticmethod
        def STRING():
            return object()

        @staticmethod
        def INT():
            return object()

    class _Row(tuple):
        def __new__(cls, *args):
            return tuple.__new__(cls, args)

    datastream.StreamExecutionEnvironment = _StreamExecutionEnvironment
    ds_functions.FlatMapFunction = _FlatMapFunction
    ds_functions.MapFunction = _MapFunction
    ds_functions.RichSinkFunction = _RichSinkFunction
    ds_functions.RichFlatMapFunction = _RichFlatMapFunction
    ds_functions.RuntimeContext = _RuntimeContext
    ds_kafka.FlinkKafkaConsumer = _FlinkKafkaConsumer
    ds_kafka.FlinkKafkaProducer = _FlinkKafkaProducer
    ds_jdbc.JdbcSink = _JdbcSink
    ds_jdbc.JdbcConnectionOptions = _JdbcConnectionOptions
    ds_jdbc.JdbcExecutionOptions = _JdbcExecutionOptions
    common_serialization.SimpleStringSchema = _SimpleStringSchema
    common_typeinfo.Types = _Types
    common.Row = _Row

    sys.modules["pyflink"] = pyflink
    sys.modules["pyflink.datastream"] = datastream
    sys.modules["pyflink.datastream.functions"] = ds_functions
    sys.modules["pyflink.datastream.connectors.kafka"] = ds_kafka
    sys.modules["pyflink.datastream.connectors.jdbc"] = ds_jdbc
    sys.modules["pyflink.common"] = common
    sys.modules["pyflink.common.serialization"] = common_serialization
    sys.modules["pyflink.common.typeinfo"] = common_typeinfo


_install_pyflink_stubs()

from infra.flink.deid_projection_job import (
    ContractViolation,
    DLQ_TOPIC,
    DeidRecord,
    PersistedVersionUpdate,
    ProjectionEvent,
    parse_debezium_record,
)


class DeidProjectionJobTests(unittest.TestCase):
    def _valid_after(self):
        return {
            "phi_id": "phi-1",
            "de_id": "de-1",
            "version": 3,
            "event_id": "evt-1",
            "request_hash": "abc",
            "favorite_color": "blue",
        }

    def _valid_envelope(self):
        return {"payload": {"after": self._valid_after()}}

    def test_constants(self):
        self.assertEqual(DLQ_TOPIC, "deid_dlq")

    def test_parse_debezium_record_from_payload_after(self):
        event = parse_debezium_record(self._valid_envelope())
        self.assertEqual(event.phi_id, "phi-1")
        self.assertEqual(event.de_id, "de-1")
        self.assertEqual(event.version, 3)
        self.assertEqual(event.favorite_color, "blue")

    def test_parse_debezium_record_from_after(self):
        envelope = {"after": self._valid_after()}
        event = parse_debezium_record(envelope)
        self.assertEqual(event.event_id, "evt-1")

    def test_parse_debezium_record_from_flat_record(self):
        event = parse_debezium_record(self._valid_after())
        self.assertEqual(event.request_hash, "abc")

    def test_parse_debezium_record_rejects_phi_fields(self):
        envelope = self._valid_envelope()
        envelope["payload"]["after"]["name"] = "Jane Doe"
        with self.assertRaises(ContractViolation):
            parse_debezium_record(envelope)

    def test_parse_debezium_record_rejects_missing_required_fields(self):
        envelope = self._valid_envelope()
        del envelope["payload"]["after"]["event_id"]
        with self.assertRaises(ContractViolation):
            parse_debezium_record(envelope)

    def test_projection_event_shape(self):
        event = ProjectionEvent("phi-1", "de-1", 42, "evt-1", "hash-1", "red")
        self.assertEqual(event.version, 42)
        self.assertEqual(event.favorite_color, "red")


if __name__ == "__main__":
    unittest.main()
