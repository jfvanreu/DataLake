import configparser
from time import time
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek, date_format, monotonically_increasing_id
from pyspark.sql.types import StructType as R, StructField as Fld, StringType as Str, DoubleType as Dbl, LongType as Long, TimestampType

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get("AWS", "AWS_ACCESS_KEY_ID")
os.environ['AWS_SECRET_ACCESS_KEY']=config.get("AWS", "AWS_SECRET_ACCESS_KEY")

def create_spark_session():
    """
    create Spark session or "grab" it if it already exists
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    spark.conf.set("mapreduce.fileoutputcommitter.algorithm.version", "2")
    return spark

def process_song_data(spark, input_data_songs, output_data):
    """
    Read song data by providing it an expected schema.
    Create songs and artists tables.
    """
    # define song data schema to improve performance
    song_schema = R([
        Fld("artist_id", Str()),
        Fld("artist_latitude", Dbl()),
        Fld("artist_location", Str()),
        Fld("artist_longitude", Dbl()),
        Fld("artist_name", Str()),
        Fld("duration", Dbl()),
        Fld("num_songs", Long()),
        Fld("song_id", Str()),
        Fld("title", Str()),
        Fld("year", Long())
    ])
        
    song_data = input_data_songs
    
    t_start=time()
    dfSongs = spark.read.json(song_data, schema=song_schema)
    t_end = time() - t_start
    print('Read song data in {} secs'.format(t_end))
    dfSongs.printSchema()
    
    dfSongs.count()    
    dfSongs.show(5)
    
    songs_table = dfSongs.filter(dfSongs.song_id != '')\
                     .select(['song_id', 'title', 'artist_id', 'year', 'duration']) 
    songs_table.show(5)
    songs_table.write.partitionBy("year", "artist_id").mode('overwrite').parquet(output_data + 'songs/songs_table.parquet')
    
    artists_table = dfSongs.filter(dfSongs.artist_id !='') \
                        .select(col("artist_id"),col("artist_name").alias("name"), col("artist_location").alias("location"),
                                 col("artist_longitude").alias("longitude"), col("artist_latitude").alias("latitude"))\
                        .dropDuplicates()
    
    artists_table.show(5)

    artists_table.write.mode('overwrite').parquet(output_data + 'artists/artists_table.parquet')

def process_log_data(spark, input_data_logs, output_data):
    """
    Read the log data using the expected schema.
    Create users, time and songplays tables.
    """
    # create log data schema to improve performance
    log_schema = R([
        Fld("artist", Str()),
        Fld("auth", Str()),
        Fld("firstName", Str()),
        Fld("gender", Str()),
        Fld("itemInSession", Long()),
        Fld("lastName", Str()),
        Fld("length", Dbl()),
        Fld("level", Str()),
        Fld("location", Str()),
        Fld("method", Str()),
        Fld("page", Str()),
        Fld("registration", Dbl()),
        Fld("sessionId", Long()),
        Fld("song", Str()),
        Fld("status", Long()),
        Fld("ts", Long()),
        Fld("userAgent", Str()),
        Fld("userId", Str())
    ])

    log_data = input_data_logs

    t_start=time()
    dfLogs=spark.read.json(log_data, schema=log_schema)   
    t_end = time() - t_start
    print('Read log data in {} secs'.format(t_end))
    
    dfLogs.printSchema()    
    dfLogs.count()    
    dfLogs.show(5)
    
     # filter NextSong records
    dfNextSongLogs=dfLogs.filter(dfLogs.page == 'NextSong')
    
    users_table = dfNextSongLogs.filter(dfNextSongLogs.userId !='') \
                        .select(col("userId").alias("user_id"),col("firstName").alias("first_name"), col("lastName").alias("last_name"), col("gender"), col("level")) \
                        .dropDuplicates()

    users_table.show(20)

    users_table.write.mode('overwrite').parquet(output_data + 'users/users_table.parquet')
    get_timestamp = udf(lambda ms: datetime.fromtimestamp(ms/1000.0), TimestampType())
    dfNextSongLogs = dfNextSongLogs.withColumn('start_time', get_timestamp('ts'))
    
    time_table = dfNextSongLogs.select('start_time')\
                            .withColumn('hour',hour('start_time')).withColumn('day',dayofmonth('start_time'))\
                            .withColumn('week',weekofyear('start_time')).withColumn('month', month('start_time'))\
                            .withColumn('year', year('start_time')).withColumn('weekday',dayofweek('start_time'))
    time_table.show(5)

    time_table.write.partitionBy("year", "month").mode('overwrite').parquet(output_data+'time/time_table.parquet')

    dfSongs = spark.read.parquet(output_data + '/songs/')

    songplays_table = dfNextSongLogs.join(dfSongs, (dfNextSongLogs.song == dfSongs.title) & (dfNextSongLogs.length == dfSongs.duration), 'left_outer')\
        .select(
            dfNextSongLogs.start_time,
            col("userId").alias('user_id'),
            dfNextSongLogs.level,
            dfSongs.song_id,
            dfSongs.artist_id,
            col("sessionId").alias("session_id"),
            dfNextSongLogs.location,
            col("useragent").alias("user_agent"),
            year('start_time').alias('year'),
            month('start_time').alias('month'))\
        .withColumn("idx", monotonically_increasing_id())

    songplays_table=songplays_table.filter("song_id is not null and artist_id is not null")
    songplays_table.show(5)

    songplays_table.write.partitionBy("year", "month").mode('overwrite').parquet(output_data+'songplays/songplays_table.parquet')

def main():
    """
    Main code block to initialize Spark session, set configuration and call reading functions.
    """
    spark = create_spark_session()
    
    location = config.get("DATA", "LOCATION")
    input_data_songs = config.get(location, "INPUT_DATA_SD")
    input_data_logs = config.get(location, "INPUT_DATA_LD")
    output_data = config.get(location, "OUTPUT_DATA")

    print("Processing song data:",input_data_songs)
    process_song_data(spark, input_data_songs, output_data)    
    
    print("Processing log data:",input_data_logs)
    process_log_data(spark, input_data_logs, output_data)


if __name__ == "__main__":
    main()
