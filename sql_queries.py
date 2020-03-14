import configparser
import boto3

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# Get roleARN
AWS_ACCESS_KEY_ID      = config.get('AWS','AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY  = config.get('AWS','AWS_SECRET_ACCESS_KEY')
    
CLUSTER_IDENTIFIER = config.get("CLUSTER","CLUSTER_IDENTIFIER")
    
redshift = boto3.client('redshift',
                       region_name="us-east-1",
                       aws_access_key_id=AWS_ACCESS_KEY_ID,
                       aws_secret_access_key=AWS_SECRET_ACCESS_KEY
                       )

myClusterProps = redshift.describe_clusters(ClusterIdentifier=CLUSTER_IDENTIFIER)['Clusters'][0]
 
roleArn = myClusterProps['IamRoles'][0]['IamRoleArn']
    

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
 CREATE TABLE staging_events (
        artist VARCHAR,
        auth VARCHAR,
        first_name VARCHAR,
        gender CHAR(1),
        session_item INT,
        last_name VARCHAR,
        length FLOAT,
        level VARCHAR,
        location VARCHAR,
        method VARCHAR,
        page VARCHAR,
        registration BIGINT,
        session_id INT,
        song VARCHAR ,
        status INT,
        ts BIGINT SORTKEY,
        user_agent VARCHAR,
        user_id INT
    )
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs (
        artist_id VARCHAR,
        artist_location VARCHAR,
        artist_latitude FLOAT,
        artist_longitude FLOAT,
        artist_name VARCHAR,
        duration FLOAT,
        num_songs INT,
        song_id VARCHAR,
        title VARCHAR,
        year INT
    )
""")

songplay_table_create = ("""
CREATE TABLE songplays (
        songplay_id INT IDENTITY(0, 1) PRIMARY KEY SORTKEY ,
        start_time TIMESTAMP NOT NULL,
        user_id VARCHAR NOT NULL,
        level VARCHAR,
        song_id VARCHAR,
        artist_id VARCHAR,
        session_id INT,
        location VARCHAR,
        user_agent VARCHAR
    )
    DISTSTYLE AUTO
""")

user_table_create = ("""
CREATE TABLE users (
        user_id VARCHAR PRIMARY KEY SORTKEY,
        first_name VARCHAR,
        last_name VARCHAR,
        gender CHAR(1),
        level VARCHAR
    )
    DISTSTYLE AUTO
""")

song_table_create = ("""
CREATE TABLE songs (
        song_id VARCHAR PRIMARY KEY SORTKEY,
        title VARCHAR,
        artist_id VARCHAR,
        year INT,
        duration INT
    )
    DISTSTYLE AUTO
""")

artist_table_create = ("""
CREATE TABLE artists (
        artist_id VARCHAR PRIMARY KEY SORTKEY,
        name VARCHAR,
        location VARCHAR,
        latitude FLOAT,
        longitude FLOAT
    )   
    DISTSTYLE AUTO
""")

time_table_create = ("""
CREATE TABLE time (
        start_time TIMESTAMP PRIMARY KEY SORTKEY,
        hour INT,
        day INT,
        week INT,
        month INT,
        year INT,
        weekday INT
    )
    DISTSTYLE AUTO
""")

# STAGING TABLES

staging_events_copy = ("""
  COPY staging_events
  FROM '{}'
  CREDENTIALS 'aws_iam_role={}'
  FORMAT AS JSON '{}'
  REGION 'us-west-2'
""").format(config.get('S3', 'LOG_DATA'), roleArn, config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""
COPY staging_songs
  FROM '{}'
  CREDENTIALS 'aws_iam_role={}'
  FORMAT AS JSON 'auto'
  REGION 'us-west-2'
""").format(config.get('S3', 'SONG_DATA'), roleArn)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT TIMESTAMP 'epoch' + (ts/1000) * INTERVAL '1 Second ' as start_time,
           user_id,
           level,
           song_id,
           artist_id,
           session_id,
           location,
           user_agent
    FROM staging_events a
    LEFT JOIN staging_songs b ON a.song = b.title AND a.artist = b.artist_name
    WHERE A.page='NextSong'
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT user_id,
           first_name,
           last_name,
           gender,
           level
    FROM staging_events
    WHERE staging_events.page='NextSong'
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT song_id,
           title,
           artist_id,
           year,
           duration 
    FROM staging_songs
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT artist_id,
           artist_name,
           artist_location,
           artist_latitude,
           artist_longitude
    FROM staging_songs
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT TIMESTAMP 'epoch' + (ts/1000) * INTERVAL '1 Second ' as start_time,
           EXTRACT(HOUR FROM start_time),
           EXTRACT(DAY FROM start_time),
           EXTRACT(WEEK FROM start_time),
           EXTRACT(MONTH FROM start_time),
           EXTRACT(YEAR FROM start_time),
           EXTRACT(DOW FROM start_time)
    FROM staging_events
    WHERE staging_events.page='NextSong'
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
