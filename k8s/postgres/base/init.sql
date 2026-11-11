CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS market;
CREATE TABLE IF NOT EXISTS market.trip_events (
 event_id text PRIMARY KEY,
 customer_id text NOT NULL,
 amount numeric(18,2) NOT NULL,
 event_time text NOT NULL
);
CREATE TABLE IF NOT EXISTS staging.trip_events
 (LIKE market.trip_events INCLUDING ALL);
