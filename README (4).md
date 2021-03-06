# Data Lake Project Using Spark


## In this project Spark was used as an ETL solution across the large Sparkify dataset. 

## Reasons to use Spark : 
#### - It is easy to use.
#### - It has the ability to partition data that result in faster reads.
#### - It supports the API for python. 
#### - It uses table partitioning to optimize reads by storing files in a hierarchy of directories based on partitioning columns. 
#### - It uses parquet files which gives the fastest read performance. 

## Project objectives :
#### The song and log Sparkify dataset are read from S3.
#### The four dimension tables (**users, songs, artists, and time**) and one fact table (**songplays**) are created from extracting data from the song and log datasets.
#### The five tables are written to partitioned parquet files in table directories on S3. 

## The following steps are taken to complete the Project objectives : 

1. Create the userid **sparkadmin** from the AWS console. 

*IAM* -> *Add user* -> *sparkadmin* -> *programmatic access* -> 
*Add policy :* **amazons3fullaccess** and **administratoraccess** -> *download the access and secret keys*

2. Fill in the **dl.cfg** file with **access** and **secret** keys :

`[AWS]`  
`AWS_ACCESS_KEY_ID='fill in access key'`  
`AWS_SECRET_ACCESS_KEY='fill in secret key'`



3. Create bucket **datalake-bucket2019** in S3. 

*S3* -> *Create Bucket* -> *enter name* **datalake-bucket2019** -> *next (keep defaults)* -> *next (keep defaults)* -> *create bucket*

4. The **etl.py** file was run to read the song and log data, transform the data to dimension and fact tables, and then write the tables to partitioned parquet files in directories on S3. 

 The following steps in **etl.py** were taken :
   
**import all the necessary spark libraries for the sparkify project**  
>import configparser
>#from datetime import datetime  
>import datetime  
import os  
from pyspark.sql import SparkSession  
from pyspark.sql.functions import udf, col  
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format  
from pyspark.sql import functions as F  

**Use the 'dl.cfg' file to get the access and secret key for reading and writing to S3** 
>config = configparser.ConfigParser()  
config.read('dl.cfg')  

>os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


>def create_spark_session():


**Create a spark session using AWS hadoop packages** 
>spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


>def process_song_data(spark, input_data, output_data):

    
**Get the song data set from the input_data directory path 's3a://udacity-dend/'** 
  
>song_data = os.path.join( input_data, "song_data/A/A/A/*.json")    
    
**Read the song data file in json format and create a spark dataframe.
    The df_songsets dataframe which will be used in the process_log_data function needs to be declared as global.**
     
>     global df_songsets
    df_songsets = spark.read.json(song_data)  

    
**The songs_table will consist of columns song_id, title, artist_id, year and duration
    that are extracted from the song dataset.** 
    
>songs_table = df_songsets['song_id', 'title', 'artist_id', 'year', 'duration']
    
**The songs table will be written to parquet files in the output_data bucket in s3
    joined by the 'songs' directory. The songs table is partitioned by year and artist.**
>output_songs = os.path.join(output_data, 'songs')
    songs_table.write.partitionBy('year', 'artist_id').parquet(output_songs)
    

**The artists_table will consist of columns artist_id, title, name, location, latitude and
    longitude that are extracted from the song dataset.** 
    
>artists_table = df_songsets.select(col("artist_id"),col("artist_name").alias("name"),
                                       col("artist_location").alias("location"),col("artist_latitude").alias("latitude"),
                                       col("artist_longitude").alias("longitude"))
    
    
**The artists table will be written to parquet files in the output_data bucket in s3
    joined by the 'artists' directory.** 
    
>output_artists = os.path.join(output_data, 'artists')
    artists_table.write.parquet(output_artists)


>def process_log_data(spark, input_data, output_data):
    
 
**Get the log data set from the input_data directory path 's3a://udacity-dend/'** 
    
>log_data = os.path.join( input_data, "log_data/*/*/*.json")    


**Read the log data file in json format and create a spark dataframe.**
    
>df_logsets = spark.read.json(log_data)  
    
**Create a dataframe df_nextsong from the log dataframe that is filtered for records with song plays
    which are those records with 'page' column equal to 'NextSong'**
    
>df_nextsong = df_logsets.filter(df_logsets["page"] == 'NextSong')
    
**The users_table will consist of columns user_id, first_name, last_name, gender, and level
    that are extracted from the log dataset. The duplicates for user_id in users_table are dropped.**
    
>users_table = df_nextsong.select(col("userId").alias("user_id"),
                                       col("firstName").alias("first_name"),col("lastName").alias("last_name"),
                                       col("gender"),col("level"))
    users_table = users_table.dropDuplicates(['user_id'])
    
    

**The users table will be written to parquet files in the output_data bucket in s3
    joined by the 'users' directory.** 
  
>output_users = os.path.join(output_data, 'users')
    users_table.write.parquet(output_users)

**Create timestamp column 'start_time' from original timestamp column 'ts' from dataframe 'df_nextsong'** 

>get_timestamp = udf(lambda x: datetime.datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d %H:%M:%S'))
    df_nextsong = df_nextsong.withColumn("start_time", get_timestamp(df_nextsong['ts']))
    
**Create datetime column 'date_time' from original timestamp column 'ts' from dataframe 'df_nextsong'** 
    
>get_datetime = udf(lambda x: datetime.datetime.fromtimestamp(x/1000.0).strftime('%Y-%m-%d'))
    df_nextsong = df_nextsong.withColumn("date_time", get_datetime(df_nextsong['ts']))
    

**From the 'df2_nextsong' dataframe the columns 'start_time' and 'date_time' will be used to 
    obtain columns 'hour', 'day', 'week', 'month', 'year', and 'weekday' to create 'time_table'**

>df_nextsong=df_nextsong.withColumn('hour', hour(df_nextsong['start_time']))
    df_nextsong=df_nextsong.withColumn('day', dayofmonth(df_nextsong['start_time']))
    df_nextsong=df_nextsong.withColumn('week', weekofyear(df_nextsong['start_time']))
    df_nextsong=df_nextsong.withColumn('month', month(df_nextsong['start_time']))
    df_nextsong=df_nextsong.withColumn('year', year(df_nextsong['start_time']))
    df_nextsong=df_nextsong.withColumn('weekday', date_format(df_nextsong['date_time'],'u'))
    time_table = df_nextsong['start_time', 'hour', 'day', 'week', 'month','year','weekday']
    

**The time table will be written to parquet files in the output_data bucket in s3
    joined by the 'time' directory. The time table is partitioned by year and month.**
    
>output_time = os.path.join(output_data, 'time')
    time_table.write.partitionBy('year', 'month').parquet(output_time)            

**The dataframe 'df_songsets' will be used for the songplays table**
>song_df = df_songsets

**Extract columns from joined song and log datasets to create songplays_table
    Use condition statement to join on artist name, song, and duration of song**

>cond = [df_nextsong.artist == song_df.artist_name, df_nextsong.song == song_df.title, 
            df_nextsong.length ==  song_df.duration]
    
**Use left outer join to create songplays_table**
>songplays_table = df_nextsong.join(song_df,cond,how='left_outer').select(df_nextsong.start_time,
    df_nextsong.userId, df_nextsong.level, song_df.song_id,
    song_df.artist_id, df_nextsong.sessionId, df_nextsong.location, df_nextsong.userAgent)
    
**Create songplays table with serial column 'songplay_id'**
>songplays_table = songplays_table.withColumn("songplay_id", F.monotonically_increasing_id() + 1)
    
**Create final songplays table where columns userId, userAgent, and sessionId are renamed.**
>songplays_table = songplays_table.select(col("songplay_id"),col("start_time"),col("userId").alias("user_id"),
                                       col("level") ,col("song_id"), col("artist_id"),col("sessionId").alias("session_id"),
                                       col("location"),col("userAgent").alias("user_agent"))

**Write songplays table to parquet files partitioned by year and month**
>songplays_table=songplays_table.withColumn('year', year(songplays_table['start_time']))
    songplays_table=songplays_table.withColumn('month', month(songplays_table['start_time']))
    
>output_songplays = os.path.join(output_data, 'songplays')
    songplays_table.write.partitionBy('year', 'month').parquet(output_songplays)

>def main():

**The main function will call the process_song_data function to write the songs and artists
    dimension tables to the s3 bucket. It will then call process_log_data to write the time and
    users dimension tables and the songplays fact table to the s3 bucket.** 
    
>spark = create_spark_session()  
>input_data = "s3a://udacity-dend/"  
>output_data = "s3a://datalake-bucket2019/"

    
>process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


>if __name__ == "__main__":
    main()    

5. Run the **etl.py** file at the terminal :  
   
   *# python etl.py*
   
6. At the AWS console in the **datalake-bucket2019** bucket there are five directories corresponding to the five tables created from running the **etl.py** file. The five tables are the **artists**, **songs**, **time**, **users** dimension tables and the **songplays** fact table. 

![Table directories](https://udacity-jpg.s3.amazonaws.com/table_directories.jpg)

7. The **songs** table files are partitioned by **year** and then **artist**. At the top of the screen of the image shown below is the path to the parquet files.
Notice that the path consists of the **year** and **artist_id** coinciding with the partitions that were created.

*Amazon S3* > *datalake-bucket2019* > *songs* > *year=1969* > *artist_id='artist_id'*

![songs table](https://udacity-jpg.s3.amazonaws.com/songs_partition.jpg)

8. The **time** table files are partitioned by **year** and **month**. At the top of the screen of the image shown below is the path to the parquet files.
Notice that the path consists of the **year** and **month** coinciding with the partitions that were created.

*Amazon S3* > *datalake-bucket2019* > *time* > *year=2018* > *month=11*

![time table](https://udacity-jpg.s3.amazonaws.com/time_partition.jpg)

9. The **songplays** table files are partitioned by **year** and **month**. At the top of the screen of the image shown below is the path to the parquet files.
Notice that the path consists of the **year** and **month** coinciding with the partitions that were created.

*Amazon S3* > *datalake-bucket2019* > *time* > *year=2018* > *month=11*

![songplays table](https://udacity-jpg.s3.amazonaws.com/songplays_partition.jpg)

10. Tables are partitioned for faster queries. The results of two queries on **songs** and **songplays** tables are shown below using the Spark **filter** command :

![songs query](https://udacity-jpg.s3.amazonaws.com/songs_query.jpg)

![songplays query](https://udacity-jpg.s3.amazonaws.com/songplays_query.jpg)

