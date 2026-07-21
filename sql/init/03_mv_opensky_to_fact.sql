CREATE MATERIALIZED VIEW default.mv_opensky_to_fact
TO default.fact_global_traffic AS
SELECT
    'opensky' AS source,
    JSONExtractString(raw_data, 'payload', 'icao24') AS icao24,
    trim(JSONExtractString(raw_data, 'payload', 'callsign')) AS callsign,
    JSONExtractString(raw_data, 'payload', 'origin_country') AS origin_country,
    toDateTime(JSONExtractInt(raw_data, 'payload', 'time_position')) AS flight_time,
    now() AS ingested_at,
    JSONExtractFloat(raw_data, 'payload', 'longitude') AS longitude,
    JSONExtractFloat(raw_data, 'payload', 'latitude') AS latitude,
    JSONExtractFloat(raw_data, 'payload', 'baro_altitude') AS altitude_m,
    JSONExtractFloat(raw_data, 'payload', 'velocity') AS velocity_ms,
    JSONExtractFloat(raw_data, 'payload', 'true_track') AS heading,
    JSONExtractBool(raw_data, 'payload', 'on_ground') AS on_ground
FROM default.kafka_opensky_raw
WHERE JSONExtractInt(raw_data, 'payload', 'time_position') > 0;