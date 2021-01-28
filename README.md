# udacity-data-engineering-spark-data-lake
## Project 3 : Data Warehouse

#### The purpose of this project is to use Redshift on Amazon Web Services as a data warehouse solution to big data. Redshift uses MPP ( Massively Parallel Processing ) architecture which use multiple cluster nodes that results in fast SQL queries. The Sparkly database which consist of song and event datasets was used to illustrate the redshift queries. 

#### The following steps were taken for this project :

1. Seven drop queries were created in **sql_queries.py** to drop seven tables (two staging tables, one fact table, and four dimension tables) as a way to refresh the database.

``` staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"```
``` staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"```

``` songplay_table_drop = "DROP TABLE IF EXISTS songplays"  ```    
``` user_table_drop = "DROP TABLE IF EXISTS users"  ```   
``` song_table_drop = "DROP TABLE IF EXISTS songs"  ```  
``` artist_table_drop = "DROP TABLE IF EXISTS artists"  ```  
``` time_table_drop = "DROP TABLE IF EXISTS time"  ```  


2. A total of seven tables were created in **sql_queries.py** Two staging tables were created (one for the **log** dataset and one for the **song** dataset ). Staging tables are necessary for easy and fast integration with the redshift cluster. The fact table (**songplays**) and four dimension tables ( **users, songs, artists, and time**) were created.
     1. The **staging_events** table was created to pull in data from the log data set.
     
     ```staging_events_table_create = "CREATE TABLE IF NOT EXISTS 
staging_events(artist varchar, auth varchar, firstName varchar, gender char(1), itemInSession int, \
lastName varchar, length float, level varchar, location varchar, method varchar, page >varchar, \
registration float, sessionId int, song text, status int, ts float, userAgent text, userId int)" ```

     2. The **staging_songs** table was created to pull in data frome the song data set. 
     
     ```staging_songs_table_create = "CREATE TABLE IF NOT EXISTS \
staging_songs(num_songs int, artist_id varchar, artist_latitude float, artist_longtitude float, \
artist_location varchar, artist_name varchar, song_id varchar, title varchar, duration float, year int)" ```

     3. The fact table (**songplays**) is the table to query the songs where the "page" column is "NextSong" (where users listen to songs). The table contains the information we are interested in.
     
     ```songplay_table_create = "CREATE TABLE IF NOT EXISTS \
songplays(songplay_id bigint identity(0,1) PRIMARY KEY UNIQUE NOT NULL, start_time timestamptz NOT NULL, \
user_id int NOT NULL, level varchar, song_id varchar, artist_id varchar, \
session_id int NOT NULL, location varchar NOT NULL, user_agent varchar \
NOT NULL)"```

     4. The dimension table (**users**) contains data that is important to the fact table.
     
     ```"CREATE TABLE IF NOT EXISTS \
users (user_id varchar PRIMARY KEY, first_name varchar, last_name varchar, \
gender char(1), level varchar NOT NULL, UNIQUE(user_id))" ```

     5. The dimension table (**song**) contains data that is important to the fact table.
     ```song_table_create = "CREATE TABLE IF NOT EXISTS \
songs (song_id varchar, title text NOT NULL, artist_id varchar NOT NULL, \
year int NOT NULL, duration float NOT NULL)" ```

     6. The dimension table (**artists**) contains data that is part of fact table. 
     
     ```"CREATE TABLE IF NOT EXISTS \
artists (artist_id varchar, name varchar NOT NULL, location varchar NOT NULL, \
latitude float, longitude float)" ```

     7. The dimension table (**time**) contains data that is part of the fact table. 
     
     ```time_table_create = "CREATE TABLE IF NOT EXISTS time (start_time timestamptz, hour int \
NOT NULL, day int NOT NULL, week int NOT NULL, month int NOT NULL, year int \
NOT NULL, weekday int NOT NULL)" ```

3. Two sql copy queries were created in **sql_queries** to copy the songs and log data from AWS s3 buckets to the staging tables (staging_events and staging_songs). For this query to work the IAM ROLE ***dwhRole*** was created using the AWS console. This role has the policy **AmazonS3ReadOnlyAccess** to allow access to S3 for both staging tables. The ARN for dwhrole is listed in the dwh.cfg file . 

     1. The ***staging_events_copy*** query lists the S3 bucket name **LOG_DATA='s3://udacity-dend/log_data'** listed in dwh.cfg. The query will use **LOG_JSONPATH='s3://udacity-dend/log_json_path.json'** file to get the data. It is easier to use this file to copy data since redshift postgre uses lowercase for it's columns. Some columns in log data are named with uppercase letters. The **LOG_JSONPATH** file creates a mapping with the data and the staging table to take care of the uppercase descrepancy.
     
     ```staging_events_copy = "COPY staging_events \
from {} \
iam_role {} \
json {}".format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH']) ```

     2. The ***staging_songs_copy*** query lists the S3 bucket name **SONG_DATA='s3://udacity-dend/song_data'** file to get the data. The json type is 'auto' which means that data will be directly copies from **SONG_DATA**. 
     
     ```staging_songs_copy = "COPY staging_songs \
from {} \
iam_role {} \
json 'auto' ".format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])```


4. The redshift cluster (**dwhcluster**) was created in the create_cluster.ipynb file. The following steps were performed :

    1. The user, ***dwhadmin***, was created using the AWS console. **Dwadmin** has AWS administrator access. This user provides **access** and **secret** keys to access resources and clients. The ***config.get*** is used to get the variables in the ***dwh.cfg*** file. The variables, **KEY, SECRET, CLUSTER_TYPE, NUM_NODES, NODE_TYPE, CLUSTER_IDENTIFIER, DB_NAME , DB_USER, DB_PASSWORD , PORT, DB_IAM_ROLE_NAME AND ARN** are derived to configure the cluster.
    
    2. The AWS resources and clients ( **ec2, s3, iam, redshift, and s3client**) are then created.
    
    3. The log and song data in the S3 buckets are listed.
    
    4. The Redshift cluster name ***dwhcluster*** was created with the ***redshift_create_cluster*** command.
    
    5. The **DB_ENDPOINT** is listed. This will be entered as **HOST** in the **dwh.cfg** file and later used for the create_tables.py  and etl.py files.
    
    6. The tcp port was opened to connect to dwhCluster by creating the security group sg-ac82dfd1 using the aws console ( click ec2 , add security group). 
    For inbound traffic , the type is **Redshift**, the protocol is **TCP**, the port is **5439** and the source is **0.0.0.0/0** ( all traffic)
    
    7. A connection is then made to the cluster using the **DB_USER, DB_PASSWORD, DB_ENDPOINT, PORT, AND DB_NAME** variables to confirm that the create cluster process was successful. 
    
5. The **create_tables.py** file is run. This file makes a connection to the cluster by obtaining the variables from **dwh.cfg**. The seven tables ( **staging_event, staging_song, songplays, users, time, songs, and artists** ) are dropped and then created by calling the queries in **sql_queries.py** . 

```drop_tables(cur, conn)```  
```create_tables(cur, conn)```

6. Data from the log and song dataset is then inserted in the fact and dimension tables in **sql_queries.py**.

**songplay_table_insert =**   
```
INSERT INTO songplays
(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
(
    SELECT TIMESTAMP 'epoch' + se.ts/1000 * interval '1 second' AS start_time,
    se.userid AS user_id, 
    se.level AS level, 
    songs.song_id AS song_id,
    artists.artist_id AS artist_id, 
    se.sessionid AS session_id, 
    se.location AS location, 
    se.useragent AS user_agent
    FROM staging_events se, artists
    LEFT JOIN songs ON songs.artist_id = artists.artist_id
    WsHERE (page = 'NextSong' AND start_time IS NOT NULL AND user_id IS NOT NULL AND song_id IS NOT
           NULL AND session_id IS NOT NULL AND user_agent IS NOT NULL)
);
```

**user_table_insert =**  
```
INSERT INTO users
(
    SELECT DISTINCT userid as user_id, firstname as first_name, lastname as last_name, gender, level
    FROM staging_events
    WHERE (staging_events.userid IS NOT NULL AND staging_events.level IS NOT NULL)
);
```

**song_table_insert =**  
```
INSERT INTO songs
(
    SELECT song_id, title, artist_id, year, duration
    FROM staging_songs
    WHERE (staging_songs.title IS NOT NULL AND staging_songs.artist_id IS NOT NULL AND
          staging_songs.year IS NOT NULL AND staging_songs.duration IS NOT NULL)
);
```

**artist_table_insert =**
```
INSERT INTO artists
(
    SELECT artist_id, artist_name AS name, artist_location AS location,
    artist_latitude AS latitude, artist_longtitude AS longtitude
    FROM staging_songs
    WHERE (staging_songs.artist_name IS NOT NULL AND staging_songs.artist_location IS NOT NULL)
);
```

**time_table_insert =**  
```
INSERT INTO time
(
  SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, 
  EXTRACT(HOUR FROM TIMESTAMP 'epoch' + ts/1000 * interval '1 second' ) AS hour,
  EXTRACT(DAY FROM TIMESTAMP 'epoch' + ts/1000 * interval '1 second' ) AS day,
  EXTRACT(WEEK FROM TIMESTAMP 'epoch' + ts/1000 * interval '1 second' ) AS week,
  EXTRACT(MONTH FROM TIMESTAMP 'epoch' + ts/1000 * interval '1 second' ) AS month,
  EXTRACT(YEAR FROM TIMESTAMP 'epoch' + ts/1000 * interval '1 second' ) AS year,
  EXTRACT(DOW FROM TIMESTAMP 'epoch' + ts/1000 * interval '1 second' ) AS weekday  
  FROM staging_events
  WHERE staging_events.ts IS NOT NULL  
);
```

7. The ***etl.py*** file is run. This file will make a connection to **dwhCluster**, run the copy queries  to load data into staging_events and staging_songs tables and then run the insert table queries to load data into the fact and dimension tables.

8. The AWS console will show the redshift cluster as available . To look at queries go to **Query Editor**. Choose public schema. All seven tables will be present with data from all seven tables. 

9. An example of a query in query editor with the results is as 
follows : 

```
New Query 1
New Query 1
dwhadmin
 
 
select count(*) from public."songplays"
WHERE level = 'free' ;
​
Use Ctrl + Enter to run query, Ctrl + Space to autocomplete
Run query Save as Save Clear Send feedback
Query results Query completed in 4.625 seconds View executionShowing row(s) 1 - 1 Download CSV
count
1 1229
```


