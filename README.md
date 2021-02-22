# Data Lake ETL project on AWS and Spark

## Introduction

The purpose of this project is to create an ETL process which runs on Spark and AWS.
The ETL process will run on the Sparkify application songs and logs data.

## Instructions

To run the ETL project, take on the following steps:

1. Update the dl.cfg file as follows:

- Set **Location** env variable to S3 or LOCAL. With S3, the ETL process will process data from and to AWS S3. If you'd like to run the ETL process locally, set Location to LOCAL.
- Set **AWS_ACCESS_KEY_ID** and **AWS_SECRET_ACCESS_KEY** to a AWS user/role which has access to an EMR instance and S3 storage.
- In S3, indicate the S3 bucket where the output data should be written to.

1. Launch the ETL.py script

- If you run the script locally (i.e. not on an EMR cluster), simply use the **python etl.py** command.
- If you run the script on an EMR cluster, first copy etl.py and dl.cfg to the cluster master machine using the scp UNIX command. See details below if needed. Then, use the **spark-submit etl.py** command.
 
In both cases, make sure dl.cfg is in the same folder as the etl.py script.

## Data schemas

As part of this project, we'll create a star schema optimized for queries on song play analysis. This includes the following tables.

**Fact Table**

- *songplays* - records in log data associated with song plays i.e. records with page NextSong
  - songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

**Dimension Tables**

- *users* - users in the app
  - user_id, first_name, last_name, gender, level
- *songs* - songs in music database
  - song_id, title, artist_id, year, duration
- *artists* - artists in music database
  - artist_id, name, location, lattitude, longitude
- *time* - timestamps of records in songplays broken down into specific units
  - start_time, hour, day, week, month, year, weekday

## Data sets

Two datasets reside in AWS S3:

**Song data:** s3://udacity-dend/song_data
**Log data:** s3://udacity-dend/log_data

We are also given a smaller dataset for both song and log data, available in the data folder. Those were given in .zip files which we extracted in the data folder using the unzip command.

## Overall strategy

This was a complex project with a lot of moving parts, so we decided to take a very methodical approach. Each section below represents a step in our all process.

### Data Lake Project Jupyter Notebook

To investigate the data and various steps required to process the data, we decided to create a Jupyter Notebook: **DataLakeProject.ipynb**. This Notebook was helpful to understand and visualize our data and verify the expected outcomes of our commands. This Notebook successfully ran on the small dataset included in the data folder.

### ETL script

Once our Jupyter Notebook was validated, we created a separate etl.py file which pretty much included all the steps from the Jupyter Notebook. We executed this script on our small dataset and verified its successful execution. All data was written to the data/output folder as expected.

### S3 bucket

The ultimate goal of this project is to write the STAR-schema tables to an AWS S3 bucket. So, we logged into AWS Console and created an S3 bucket using the user interface. We also included an input and output folders to organize our data.

### EMR cluster

We created an EMR cluster (with configuration 5.32) including Spark. We opted for a M5.2XLarge configuration which ended up performing quite well with our data load. We use AWS Console to create the EMR cluster. Initially, we had used 5.20 but faced some issues because it included Python 2.7. We needed to make sure it included the right version of Python (3+).

### Jupyter Notebook on EMR cluster

Once the EMR cluster was up and running, we could also create a new Jupyter Notebook on EMR this time. The Notebook is actually stored in S3, so it is persistent even after we delete our cluster.
We transferred our scripts and notebook to this new Jupyter environment and verified that our script was still running fine. This time, instead of using local data, we also explored the large data set stored on S3. To do so, change the **location** configuration field in dl.cfg to S3.

### ETL script on EMR cluster

Finally, once we validated the Jupyter Notebook on EMR, we copied the etl.py and dl.cfg files to the EMR master node (/home/hadoop/ folder). We used the following commands to do so:

- scp -i ~/.aws/sparky.pem **etl.py** ec2-54-188-102-211.us-west-2.compute.amazonaws.com:/home/hadoop/
- scp -i ~/.aws/sparky.pem **dl.cfg** ec2-54-188-102-211.us-west-2.compute.amazonaws.com:/home/hadoop/

scp is a unix command to copy across machines. We provide our public key (sparky.pem) to connect to the running EMR master machine.

We need to make sure both files are present in order to run the script. Initially, we used an EMR 5.20 cluster but some Python packages were missing, so we ended up using EMR 5.32. To run the python script through our Spark cluster, we executed the following command.

To run the script, simply type: **spark-submit etl.py**

## Lessons learned

In this section, we highlight some of the lessons learned during this project.

### Step by step process

The all environment was quite complex, so it's better to start small and create a Jupyter Notebook to analyze the data and make sure everything works well on the small data set. Once we achieve the expected results on the small data set, we can upgrade to the larger dataset and infrastructure.

### Data schema

In the Jupyter Notebook, we loaded the small datasets. This gave us visibility on the data schemas. 
We used those data schemas as part of the Spark "read" operation for the large dataset. This accelerates the data reading operation. This was a recommendation on the student forum.

### Spark.conf.set

To seed up the process, some students also recommended to use the following configuration to accelerate the writing (output) process: **Spark.conf.set("mapreduce.fileoutputcommitter.algorithm.version", "2")**

spark.sparkContext.hadoopConfiguration.set("mapreduce.fileoutputcommitter.algorithm.version", "2")" is a fileoutputcomitter algorithm. Spark uses FileOutputCommitter to manage staging output files and final output files. The behavior of FileOutputCommitter has direct impact on the performance of jobs that write data.

### EMR configuration

Because we took this step-by-step approach, we actually didn't need to spend as much time on EMR. We spent a lot of work making our script work with Spark Local. Once we were ready for production, we decided to opt for a larger EMR infrastructure (M5.2XLarge). This seems to pay off since we completed our entire process in about 7'30" while many students seem to need 30 minutes.

`
[hadoop@ip-172-31-23-138 ~]$ spark-submit etl.py
21/02/21 23:20:30 INFO SparkContext: Running Spark version 2.4.7-amzn-0
(many lines of code)
21/02/21 23:27:59 INFO SparkContext: Successfully stopped SparkContext
`

## Improvement opportunities

While we meet the project requirements, this project could be improved. Here are a couple of suggestions:

### Optimization (copy to S3 once work is completed on HDFS)

In our current process, we immediately copy our dimension tables to S3. It would be interesting to investigate if we could accelerate our process by creating our output data on the cluster (in HDFS) and then copy it to S3.

### Spark Logging clean-up

Spark prints a lot of information while running our etl.py script. It would be beneficial to turn off some of those logs to improve the visibility of our output.
