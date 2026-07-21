CREATE TABLE default.fact_global_traffic (
    source LowCardinality(String),
    icao24 String,
    callsign String,
    origin_country LowCardinality(String),
    flight_time DateTime,
    ingested_at DateTime DEFAULT now(),
    longitude Nullable(Float64),
    latitude Nullable(Float64),
    altitude_m Nullable(Float32),
    velocity_ms Nullable(Float32),
    heading Nullable(Float32),
    on_ground UInt8
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(flight_time)
ORDER BY (flight_time, icao24)
TTL flight_time + INTERVAL 3 DAY;