BEGIN;
CREATE TABLE "station_station" (
    "id" integer NOT NULL PRIMARY KEY,
    "latitude" real NOT NULL,
    "longitude" real NOT NULL,
    "altitude" real NOT NULL,
    "mesonet_id" varchar(25) NOT NULL,
    "name" varchar(50) NOT NULL
)
;
CREATE TABLE "station_observation" (
    "id" integer NOT NULL PRIMARY KEY,
    "station_id" integer NOT NULL REFERENCES "station_station" ("id"),
    "timestamp" datetime NOT NULL,
    "temperature" real NOT NULL,
    "sknt" real NOT NULL,
    "direction" real NOT NULL,
    "gust" real NOT NULL,
    "pmsl" real NOT NULL,
    "dewpoint" real NOT NULL,
    "relhumidity" real NOT NULL,
    "weather" real NOT NULL,
    "p24" real NOT NULL
)
;

COMMIT;
