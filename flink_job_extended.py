from pyflink.common import Configuration, SimpleStringSchema, WatermarkStrategy, Types, Time, Duration
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.datastream.functions import ProcessWindowFunction
from pyflink.table import StreamTableEnvironment, Schema, DataTypes
from pyflink.table.udf import udf
 
import config
from metrics import average, median, format_minute, comfort_status
from utils.jars import KAFKA_CONNECTOR, JDBC_CONNECTOR, PG_DRIVER
from utils.iot_event_time import parse_iot, IotTimestampAssigner


class IotWindowFunction(ProcessWindowFunction):
    def process(self, key, context, elements):
        temps = []
        hums = []
        for e in elements:
            temps.append(e[2])
            hums.append(e[3])
 
        avg_temp = round(average(temps), 2)
        med_hum = round(median(hums), 2)
        min_temp = round(min(temps), 2)
        max_temp = round(max(temps), 2)
        cnt = len(temps)
        hhmm = format_minute(context.window().start)
 
        yield hhmm, key, avg_temp, med_hum, min_temp, max_temp, cnt


comfort_status_udf = udf(
    comfort_status,
    result_type = DataTypes.STRING(),
    input_types = [DataTypes.DOUBLE(), DataTypes.DOUBLE()]
)


def main():
    conf = Configuration()
    conf.set_integer("rest.port", 8081)
    conf.set_string("execution.runtime-mode", "STREAMING")
 
    env = StreamExecutionEnvironment.get_execution_environment(conf)
    env.set_parallelism(1)
    env.add_jars(KAFKA_CONNECTOR, JDBC_CONNECTOR, PG_DRIVER)
    tenv = StreamTableEnvironment.create(env)
 
    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers(config.KAFKA_BROKER) \
        .set_topics(config.TOPIC_IN) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .build()

    ds = env.from_source(kafka_source, WatermarkStrategy.no_watermarks(), "kafka src") \
        .map(parse_iot, output_type = Types.TUPLE([Types.INT(), Types.LONG(), Types.DOUBLE(), Types.DOUBLE()])) \
        .filter(lambda x: x is not None) \
        .assign_timestamps_and_watermarks(
            WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(5))
            .with_timestamp_assigner(IotTimestampAssigner()))
 
    agg_ds = ds \
        .key_by(lambda x: x[0]) \
        .window(TumblingEventTimeWindows.of(Time(config.WINDOW_SECONDS * 1000))) \
        .process(
            IotWindowFunction(),
            output_type=Types.TUPLE([
                Types.STRING(), Types.INT(),
                Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.INT()
            ]))
 
    t_agg = tenv.from_data_stream(
        agg_ds,
        Schema.new_builder()
        .column("f0", DataTypes.STRING())
        .column("f1", DataTypes.INT())
        .column("f2", DataTypes.DOUBLE())
        .column("f3", DataTypes.DOUBLE())
        .column("f4", DataTypes.DOUBLE())
        .column("f5", DataTypes.DOUBLE())
        .column("f6", DataTypes.INT())
        .column_by_expression("proc_time", "PROCTIME()")
        .build())
    tenv.create_temporary_view("agg", t_agg)
    tenv.create_temporary_system_function("ComfortStatus", comfort_status_udf)

    tenv.execute_sql(f"""
        CREATE TABLE device_types (
            id INT,
            type_name STRING
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{config.PG_URL}',
            'table-name' = '{config.PG_REF_TABLE}',
            'username' = '{config.PG_USER}',
            'password' = '{config.PG_PASSWORD}'
        )
    """)

    tenv.execute_sql(f"""
        CREATE TABLE iot_out (
            event_time STRING, device_type STRING,
            avg_temp DOUBLE, median_humidity DOUBLE,
            min_temp DOUBLE, max_temp DOUBLE,
            readings_count INT, status STRING
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{config.TOPIC_OUT}',
            'properties.bootstrap.servers' = '{config.KAFKA_BROKER}',
            'format' = 'csv'
        )
    """)

    select_sql = """
        SELECT a.f0, d.type_name, a.f2, a.f3, a.f4, a.f5, a.f6, ComfortStatus(a.f2, a.f3)
        FROM agg AS a
        JOIN device_types FOR SYSTEM_TIME AS OF a.proc_time AS d
          ON a.f1 = d.id
    """

    tenv.execute_sql(f"INSERT INTO iot_out {select_sql}").wait()


if __name__ == '__main__':
    main()