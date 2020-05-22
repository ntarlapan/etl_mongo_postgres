# ETL from Mongo to Postgres

This project is a service for synchronizing data from Mongo
 into a data wharehouse in Postgres. 
 
 1. First the service
 loads data from a zip archive containing orders and users.
 2. Then, gets all records up to a date (by default 2020-01-01)
 and writes them to Postgres. 
 3. On further runs, the app
 synchronizes orders that have been updated since the last
 synchronization (the field "updated_at").
 
 
This Readme provides instructions on how to
 set up the environent, install dependencies, 
 run the etl job and schedule the job to run at 
 predefined time intervals (every 5 minutes).

The loading and synchronization etl writes
 log in the file etl_app.log .
This log file could be analysed to assess the etl process.


## Prepare the databases and environment variables

### export the environment variables:
Either include the path to the zip file with data
in .env file or export the variable:

`export DATA_ZIP_PATH=path/to/zip_file/data.zip`

If provideed with an .env file check the provided .env file. 
Update values where needed, for example, if you want to use other 
connection data (host, ip) for database.
Place the .env file in the same directory as app.py file.


### Start the databases

#### option 1: docker-compose
After including the .env file (please see the previous step above);
cd into projects directory (containing docker-compose.yml)
and run:

`docker-compose up --build -d`

This command will build 2 containers: 
1 with mongodb
1 with postgres

#### option 2: existing running Mongo and Postgres 
Provide connection data to running
Mongo and Postgres in .env file. 

**Word of caution**: If you choose to provide the database yourself, 
please be aware that the script for loading data from CSV 
into Mongo erases/resets data for some collections. 
Given this, it is better NOT to connect to a database with important data.
Should you want to provide your databases, better
 create new databases for this goal.

# Install system packages and python packages

cd into the project's directory (the one containing the app.py)

create virtual environment

`python3 -m venv venv`

activate virtual enviroment:

`source venv/bin/activate`

install system requirements:

```
sudo apt-get install  libpq-dev python-dev
sudo apt-get install psycopg2
pip install  psycopg2
```

Install the requirements from file:

`pip install -r requirements.txt`

Load the data from CSV into Mongo:
`python ingest_into_mongo.py`

To run the etl job manually, execute:
`python app.py`

### How to schedule the current crontab_job: 
Make sure you are in project directory or 
cd into project directory, then:

`flask crontab add`

To show current crontab jobs with their hash codes:

`flask crontab show`

To run a job select the has and run repeatedly, 
without waiting for the time interval to occur:

`flask crontab run <job_hash>`

To remove the cron job (Purge all jobs managed by current app),
 if it is no longer needed to run automatically, 
 execute:

`flask crontab remove`