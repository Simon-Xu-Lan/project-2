DROP TABLE IF EXISTS scooters;

CREATE TABLE scooters (
	id SERIAL PRIMARY KEY,
	company VARCHAR(10),
	last_updated BIGINT,
	bike_id TEXT,
	lat NUMERIC(10,7),
	lon NUMERIC(10,7),
	is_reserved BOOLEAN,
	is_disabled BOOLEAN
);

-- CREATE TABLE weather_records (
-- 	id SERIAL PRIMARY KEY,
-- 	AIRTEMP VARCHAR(10),
-- 	RELATIVEHUMIDITY VARCHAR(10),
-- 	VISIBILITY VARCHAR(10),
-- 	WINDSPEED VARCHAR(10),
-- 	DATADATETIME BIGINT,
-- 	tractid BIGINT
-- );

-- CREATE TABLE process_log (
-- 	id SERIAL PRIMARY KEY,
-- 	last_saved BIGINT,
-- 	saved_records INTEGER
-- );


