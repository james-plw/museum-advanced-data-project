# Museum Data Project

* In this project,

## Setup

1. Create a venv, activate it and run 'pip3 install -r requirements.txt' to install required libraries
2. Create a .env with the following details - BOOTSTRAP_SERVERS, USERNAME, PASSWORD to connect to Kafka, and also choose a GROUP (e.g. 1).
3. The .env should also contain details to connect to your RDS - DATABASE_USERNAME, DATABASE_PASSWORD, DATABASE_IP, DATABASE_PORT & DATABASE_NAME.

## Setup database - reset.sh
* run 'bash rest.sh' to reset the database and create tables with the necessary base data


## Run pipeline - loading.py

* This file can perform the full pipeline: from reading and cleaning messages from Kafka, to uploading the transformed data to a Postgres database.
* Command line arguments - When running you *need* to specify where to log to with __file__ or __console__ at the end of the command line e.g. "python3 loading.py console". Also use -h for more info
* To run as a background task - run 'nohup python3 loading.py console &'

### What the script does
1. Reads messages from 'lmnh' topic - logs invalid messages and prints valid messages
2. Every 100 valid messages are turned into a dataframe which is transformed into two dataframes, one for requests (assistance, emergency) and one for ratings (0-4)
3. Connects to the database, creates a temporary csv file for the dataframe and then copies from it into the database. This is repeated for both dataframes

