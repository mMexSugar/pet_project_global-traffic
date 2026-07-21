CREATE TABLE default.kafka_opensky_raw (
    raw_data String
) ENGINE = Kafka
SETTINGS 
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'raw.traffic.opensky',
    kafka_group_name = 'clickhouse_opensky_consumer_v1',
    kafka_format = 'JSONAsString';