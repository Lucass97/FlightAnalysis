//Create keyspace
CREATE KEYSPACE IF NOT EXISTS StreamingKeySpace WITH replication = {'class':'SimpleStrategy', 'replication_factor':3};
CREATE KEYSPACE IF NOT EXISTS BatchKeySpace WITH replication = {'class':'SimpleStrategy', 'replication_factor':3};

//Create tables
CREATE TABLE IF NOT EXISTS StreamingKeySpace.Flight_Data ("FL_DATE" date , "OP_CARRIER" text, "OP_CARRIER_FL_NUM" bigint, "ORIGIN" text, "DEST" text, "CRS_DEP_TIME" bigint, "DEP_TIME" bigint, "DEP_DELAY" bigint, "TAXI_OUT" bigint, "WHEELS_OFF" bigint, "WHEELS_ON" bigint, "TAXI_IN" bigint, "CRS_ARR_TIME" bigint, "ARR_TIME" bigint, "ARR_DELAY" bigint, "CANCELLED" bigint, "CANCELLATION_CODE" text, "DIVERTED" bigint, "PLANNED_ELAPSED_TIME" bigint, "ELAPSED_TIME" bigint, "AIR_TIME" bigint, "DISTANCE" bigint, "CARRIER_DELAY" bigint, "WEATHER_DELAY" bigint, "NAS_DELAY" bigint, "SECURITY_DELAY" bigint, "LATE_AIRCRAFT_DELAY" bigint, PRIMARY KEY ("FL_DATE", "OP_CARRIER", "OP_CARRIER_FL_NUM"));

CREATE TABLE IF NOT EXISTS StreamingKeySpace.Delay_Data ("OP_CARRIER" text, "MONTH" bigint, "DEP_DELAY" float, "ARR_DELAY" float, primary key ("OP_CARRIER", "MONTH"));


CREATE TABLE IF NOT EXISTS BatchKeySpace.delay_total ("OP_CARRIER" text, "AVG_DEP_DELAY" float, "AVG_ARR_DELAY" float, primary key ("OP_CARRIER"));
CREATE TABLE IF NOT EXISTS BatchKeySpace.delay_year ("OP_CARRIER" text,  "YEAR" bigint, "AVG_DEP_DELAY" float, "AVG_ARR_DELAY" float, primary key ("OP_CARRIER", "YEAR"));
CREATE TABLE IF NOT EXISTS BatchKeySpace.delay_year_month ("OP_CARRIER" text, "YEAR" bigint, "MONTH" bigint, "AVG_DEP_DELAY" float, "AVG_ARR_DELAY" float, primary key ("OP_CARRIER", "YEAR", "MONTH"));
CREATE TABLE IF NOT EXISTS BatchKeySpace.delay_dayofweek ("OP_CARRIER" text, "DAY_OF_WEEK" bigint, "AVG_DEP_DELAY" float, "AVG_ARR_DELAY" float, primary key ("OP_CARRIER", "DAY_OF_WEEK"));

CREATE TABLE IF NOT EXISTS BatchKeySpace.delay_total_src_dest ("ORIGIN" text, "DEST" text, "AVG_DEP_DELAY" float, "AVG_ARR_DELAY" float,primary key ("ORIGIN", "DEST"));
CREATE TABLE IF NOT EXISTS BatchKeySpace.delay_year_src_dest("ORIGIN" text, "DEST" text, "YEAR" bigint, "AVG_DEP_DELAY" float, "AVG_ARR_DELAY" float,primary key ("ORIGIN", "DEST", "YEAR"));
CREATE TABLE IF NOT EXISTS BatchKeySpace.delay_year_month_src_dest ("ORIGIN" text, "DEST" text, "YEAR" bigint, "MONTH" bigint, "AVG_DEP_DELAY" float, "AVG_ARR_DELAY" float,primary key ("ORIGIN", "DEST", "YEAR", "MONTH"));
CREATE TABLE IF NOT EXISTS BatchKeySpace.delay_dayofweek_src_dest ("ORIGIN" text, "DEST" text, "DAY_OF_WEEK" bigint,"AVG_DEP_DELAY" float, "AVG_ARR_DELAY" float,primary key ("ORIGIN", "DEST", "DAY_OF_WEEK"));

CREATE TABLE IF NOT EXISTS BatchKeySpace.cancellation_diverted_total_src_dest ("ORIGIN" text, "DEST" text, "CANC_PERC" float, "CANC_COUNT" bigint, "DIV_PERC" float, "DIV_COUNT" bigint, primary key ("ORIGIN", "DEST"));
CREATE TABLE IF NOT EXISTS BatchKeySpace.cancellation_diverted_year_src_dest ("ORIGIN" text, "DEST" text, "YEAR" bigint, "CANC_PERC" float, "CANC_COUNT" bigint, "DIV_PERC" float, "DIV_COUNT" bigint, primary key ("ORIGIN", "DEST", "YEAR"));
CREATE TABLE IF NOT EXISTS BatchKeySpace.cancellation_diverted_year_month_src_dest("ORIGIN" text, "DEST" text, "YEAR" bigint, "MONTH" bigint, "CANC_PERC" float, "CANC_COUNT" bigint, "DIV_PERC" float, "DIV_COUNT" bigint, primary key ("ORIGIN", "DEST", "YEAR", "MONTH"));
CREATE TABLE IF NOT EXISTS BatchKeySpace.cancellation_diverted_dayofweek_src_dest("ORIGIN" text, "DEST" text, "DAY_OF_WEEK" bigint, "CANC_PERC" float, "CANC_COUNT" bigint, "DIV_PERC" float, "DIV_COUNT" bigint, primary key ("ORIGIN", "DEST", "DAY_OF_WEEK"));

CREATE TABLE IF NOT EXISTS BatchKeySpace.cancellation_diverted_total("OP_CARRIER" text,  "CANC_PERC" float, "CANC_COUNT" bigint, "DIV_PERC" float, "DIV_COUNT" bigint, primary key ("OP_CARRIER"));
CREATE TABLE IF NOT EXISTS BatchKeySpace.cancellation_diverted_year("OP_CARRIER" text, "YEAR" bigint, "CANC_PERC" float, "CANC_COUNT" bigint, "DIV_PERC" float, "DIV_COUNT" bigint, primary key ("OP_CARRIER","YEAR"));
CREATE TABLE IF NOT EXISTS BatchKeySpace.cancellation_diverted_year_month("OP_CARRIER" text, "YEAR" bigint, "MONTH" bigint, "CANC_PERC" float, "CANC_COUNT" bigint, "DIV_PERC" float, "DIV_COUNT" bigint, primary key ("OP_CARRIER","YEAR", "MONTH"));
CREATE TABLE IF NOT EXISTS BatchKeySpace.cancellation_diverted_dayofweek("OP_CARRIER" text, "DAY_OF_WEEK" bigint, "CANC_PERC" float, "CANC_COUNT" bigint, "DIV_PERC" float, "DIV_COUNT" bigint, primary key ("OP_CARRIER","DAY_OF_WEEK"));

CREATE TABLE IF NOT EXISTS BatchKeySpace.dist_total("OP_CARRIER" text, "TOTAL_DISTANCE" float, primary key ("OP_CARRIER"));
CREATE TABLE IF NOT EXISTS BatchKeySpace.dist_year("OP_CARRIER" text, "YEAR" bigint, "TOTAL_DISTANCE" float, primary key ("OP_CARRIER", "YEAR"));
CREATE TABLE IF NOT EXISTS BatchKeySpace.dist_year_month("OP_CARRIER" text, "YEAR" bigint, "MONTH" bigint, "TOTAL_DISTANCE" float, primary key ("OP_CARRIER", "YEAR", "MONTH"));
CREATE TABLE IF NOT EXISTS BatchKeySpace.dist_dayofweek ("OP_CARRIER" text, "DAY_OF_WEEK" bigint, "TOTAL_DISTANCE" float, primary key ("OP_CARRIER", "DAY_OF_WEEK"));

CREATE TABLE IF NOT EXISTS BatchKeySpace.max_consec_delay_year ("OP_CARRIER" text, "YEAR" bigint, "MAX_GIORNI" bigint, primary key ("OP_CARRIER", "YEAR"));
CREATE TABLE IF NOT EXISTS BatchKeySpace.max_consec_delay_year_src_dest("ORIGIN" text, "DEST" text, "YEAR" bigint, "MAX_GIORNI" bigint, primary key ("ORIGIN", "DEST", "YEAR"));

CREATE TABLE IF NOT EXISTS BatchKeySpace.src_dest_canc_code ("ORIGIN" text, "DEST" text, "YEAR" bigint,"CANCELLATION_CODE" text, "NUM_CANCELLED" bigint, primary key ("ORIGIN", "DEST", "YEAR", "CANCELLATION_CODE"));

//Select from table
SELECT * FROM StreamingKeySpace.Flight_Data;

