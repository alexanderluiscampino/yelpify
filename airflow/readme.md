# Data Pipelines with Airflow

## Objective

To build a state of the art data pipeline using Airflow to be able to run schedules jobs agains the data produced by the app users and load it into analytics tables in order to derive useful infomation out of them.


## Datasets

Datasets are publicly avaialable in the following S3 Bucket Locations:
   * Event Data: `s3://udacity-dend/log_data`
   * Songs Data: `s3://udacity-dend/song_data`

These are JSON files, with known schemas and partitioned in S3 by year/month, which is extremely useful to load only portions of the data and not the whole data set everytime, since the size of these datasets will grow tremendously in the future

## Airflow DAG
The image below shows the DAG representing this pipeline.

![Fig 1: Dag with correct task dependencies](sparkify_dag.png)
 
Custom operators were created to handle specific tasks related to this data pipeline.

- The following parameters are set as default for this data pipeline:
    * Dag does not have dependencies on past runs
    * On failure, tasks are retried 3 times
    * Retries happen every 5 minutes
    * Catchup is turned off
    * Do not email on retry

### Dag Components
- The 2 custom componenets developed for this application are:
1. Operators
    * Load Data to Stage Redshift Operator
    * Fact Table Loader Operator
    * Dimensional Table Loader Operator
    * Data quality Checker Operator

2. Helper class with SQL
    * This helpwer will contain all SQL language transformations to apply on the staging tables and insert to the fact & dimensional tables


1. Load Data to Stage Redshift Operator 
    * Loads JSON files from S3 to Amazon Redshift
    * Creates and runs a `SQL COPY` statement based on the parameters provided
    * Parameters should specify where in S3 file resides and the target table
    * Contain a templated field that allows it to load timestamped files from S3 based on the execution time and run backfills
2. Fact & Dimensional Table Loader Operators
    * Receives a Create and Insert query and runs those queries
    * Create query will have the table definition to create the table if it does not exist
    * The insert query will contain the logic to transform the staging tables to useful Fact or Dimensional tables
    * Modes of operation:
        * Append: Data is appended, existing table data is not altered
        * Truncate: Data in the table is removed and new data is inserted. This action is destructive.marked

3. Data quality Checker Operator
    * Run checks on the data
    * Runs a simple checker for counting records on table. This can certainly be extended to run more checks
    * Will raise Exception if no records were added to the table

**Observation:** Change file `dag_config.json` accordingly to the type of run necessary (apppending or truncating). Also, to change location of source data

### Airflow Variables and Connections:

#### Variables:
Load file `dag_config.json` to the avriables section of the Airlfow UI to have these variables avaialble at runtime

### Connections

The following connections need to be set up in the Airflow UI:
* *aws_credentials* -  Specific aws credentials to be used:
    * SECRET_ACCESS_KEY
    * SECRET_ACCESS_ID


* *redshift* -  Redshift Cluster information:
    * Host
    * User
    * Password
    * Database
    * Port

## Build Instructions

For local testing purposes, run Airflow locally using the shell script file `run_airflow.sh`. Docker will be needed. The following docker [image][https://hub.docker.com/r/puckel/docker-airflow/) is used.


### Installing Docker
Recommend to install docker in windows using [WSL2](https://adamtheautomator.com/windows-subsystem-for-linux/) procedure.

Follow this [link](https://itnext.io/install-docker-on-windows-10-home-d8e621997c1d) if you have trouble installing [Docker](https://www.docker.com/products/docker-desktop) on Windows 10 Home Edition

Follow [these](https://towardsdatascience.com/getting-started-with-airflow-using-docker-cd8b44dbff98) instruction to get Airflow running locally on a docker container
