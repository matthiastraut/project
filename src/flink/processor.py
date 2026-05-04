import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

# Ensure required jar files for Kafka and FileSystem connectors are downloaded
# Typically you need flink-sql-connector-kafka, flink-connector-files

def main():
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # Use absolute paths for Jars
    curr_dir = os.path.abspath(os.getcwd())
    kafka_jar = os.path.join(curr_dir, "lib/flink-sql-connector-kafka-3.1.0-1.18.jar")
    gcs_jar = os.path.join(curr_dir, "lib/flink-gs-fs-hadoop-1.18.1.jar")
    
    print(f"Loading Kafka Jar: {kafka_jar}")
    print(f"Loading GCS Jar: {gcs_jar}")
    
    # Add jars to the environment
    env.add_jars(f"file://{kafka_jar}", f"file://{gcs_jar}")
    
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)
    
    # Set log level to INFO to see more details
    t_env.get_config().set("pipeline.jars", f"file://{kafka_jar};file://{gcs_jar}")
    
    # Configure GCS access with more Hadoop properties
    key_path = os.path.join(curr_dir, "gcp.json")
    t_env.get_config().set("fs.gs.auth.service.account.json.keyfile", key_path)
    t_env.get_config().set("fs.gs.project.id", "zoomcamp-de-project-495316")
    t_env.get_config().set("fs.gs.auth.service.account.enable", "true")
    t_env.get_config().set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    t_env.get_config().set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    
    import time
    debug_group_id = f"debug-group-{int(time.time())}"
    
    # 1. Define Kafka Source Table
    t_env.execute_sql(f"""
        CREATE TABLE kafka_source (
            date_id INT,
            time_id INT,
            symbol_id INT,
            weight DOUBLE,
            feature_00 DOUBLE,
            feature_01 DOUBLE,
            responder_6 DOUBLE
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'market-data-raw',
            'properties.bootstrap.servers' = '127.0.0.1:29092',
            'properties.group.id' = '{debug_group_id}',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json',
            'json.ignore-parse-errors' = 'true'
        )
    """)

    # 2. Define Print Sink for Debugging
    t_env.execute_sql("""
        CREATE TABLE print_sink (
            date_id INT,
            time_id INT,
            symbol_id INT,
            weight DOUBLE,
            feature_00 DOUBLE,
            feature_01 DOUBLE,
            responder_6 DOUBLE
        ) WITH (
            'connector' = 'print'
        )
    """)

    print(f"Flink Job Submitted with group.id={debug_group_id}! Monitoring market data stream...", flush=True)
    
    # 3. Simple Insert to Print Sink (Sampled at .001% for monitoring)
    table_result = t_env.execute_sql("""
        INSERT INTO print_sink
        SELECT 
            date_id,
            time_id,
            symbol_id,
            weight,
            feature_00,
            feature_01,
            responder_6
        FROM kafka_source
        WHERE RAND() < 0.00001
    """)
    
    table_result.wait()

if __name__ == '__main__':
    # To run this, you must have the Kafka SQL connector JAR in your PyFlink lib directory
    # e.g., flink-sql-connector-kafka-1.18.1.jar
    main()
