
import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES
#  the column names of staging tables should match exactly with that of the source files
staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events(
       event_id INT IDENTITY(0,1) PRIMARY KEY,
       artist_name VARCHAR,
       auth VARCHAR,
       user_first_name VARCHAR,
       user_last_name VARCHAR,
       user_gender VARCHAR,
       item_in_Session INT,
       song_length NUMERIC,
       user_level VARCHAR,
       method VARCHAR,
       page VARCHAR,
       registration NUMERIC,
       session_id INT,
       song_title VARCHAR,
       status INT,
       ts VARCHAR,
       user_agent VARCHAR,
       user_id INT,
       location VARCHAR
);
""")


staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs(
       song_id VARCHAR PRIMARY KEY,
       num_songs INT,
       artist_id VARCHAR,
       artist_latitude NUMERIC,
       artist_longitude NUMERIC,
       artist_location VARCHAR,
       artist_name VARCHAR,
       title VARCHAR,
       duration NUMERIC,
       year INT
);
""")



user_table_create = ("""
CREATE TABLE IF NOT EXISTS users(
       user_id INT PRIMARY KEY,
       first_name VARCHAR,
       last_name VARCHAR,
       gender VARCHAR,
       level VARCHAR
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs(
       song_id VARCHAR PRIMARY KEY,
       title VARCHAR,
       artist_id VARCHAR,
       year INT,
       duration NUMERIC
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists(
       artist_id VARCHAR PRIMARY KEY,
       location VARCHAR,
       latitude NUMERIC,
       longitude NUMERIC,
       name VARCHAR
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time(
       start_time TIMESTAMP PRIMARY KEY,
       hour INT,
       day INT,
       week INT,
       month INT,
       year INT,
       weekday VARCHAR
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays(
       songplay_id INT IDENTITY(0,1) PRIMARY KEY,
       start_time TIMESTAMP NOT NULL,
       user_id INT NOT NULL,
       level VARCHAR,
       song_id VARCHAR NOT NULL,
       artist_id VARCHAR NOT NULL,
       session_id INT,
       location VARCHAR,
       user_agent VARCHAR
);
""")

# STAGING TABLES
# Load from JSON Arrays Using a JSONPaths file (LOG_JSONPATH),
# setting COMPUPDATE, STATUPDATE to speed up COPY

staging_events_copy = ("""COPY staging_events 
                          FROM{}
                          CREDENTIALS 'aws_iam_role={}'
                          REGION 'us-west-2'
                          JSON{};
                       """).format(config.get('S3','LOG_DATA'), config.get('IAM_ROLE','ARN'), config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""COPY staging_songs_copy
                         FROM{}
                         CREDENTIALS 'aws_iam_role={}'
                         REGION 'us-west-2'
                         JSON 'auto';
                       """).format(config.get('S3','SONG_DATA'), config.get('IAM_ROLE','ARN'))

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT TO_TIMESTAMP(se.ts, 'YYYY-MM-DD HH24:MI:SS') AS start_time,
       se.user_id,
       se.user_level,
       ss.song_id,
       ss.artist_id,
       se.session_id,
       se.location,
       se.user_agent     
FROM staging_songs ss, staging_events se
WHERE ss.title = se.song_title
  AND ss.artist_name = se.artist_name
  AND ss.duration = se.song_length
  AND se.page = 'NextSong';
""")

user_table_insert = (""" 
INSERT INTO users(user_id, first_name, last_name, gender, level)
SELECT DISTINCT 
        user_id,
        user_first_name,
        user_last_name,
        user_gender, 
        user_level
    FROM staging_events
    WHERE page = 'NextSong'
    AND user_id NOT IN (SELECT DISTINCT user_id FROM users)
""")

song_table_insert = ("""
INSERT INTO songs(song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id,
       title,
       artist_id,
       year,
       duration
FROM staging_songs
WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude) 
    SELECT DISTINCT 
        artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    FROM staging_songs
    WHERE artist_id NOT IN (SELECT DISTINCT artist_id FROM artists)
""")


time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT 
        start_time, 
        EXTRACT(hr from start_time) AS hour,
        EXTRACT(d from start_time) AS day,
        EXTRACT(w from start_time) AS week,
        EXTRACT(mon from start_time) AS month,
        EXTRACT(yr from start_time) AS year, 
        EXTRACT(weekday from start_time) AS weekday 
    FROM (
        SELECT DISTINCT  TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as start_time 
        FROM staging_events s     
    )
    WHERE start_time NOT IN (SELECT DISTINCT start_time FROM time)
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
