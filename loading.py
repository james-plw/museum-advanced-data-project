# pylint: disable=broad-exception-caught, logging-fstring-interpolation, bare-except

'''Module dedicated to extracting data from kafka,
taking every 100 valid messages, transforming them into two dataframes,
and uploading to a postgres database'''
from os import environ, remove
import json
import logging
import argparse
from datetime import datetime, time
import pandas as pd
from dotenv import load_dotenv
from confluent_kafka import Consumer
from psycopg2.extensions import connection
from psycopg2 import connect, DatabaseError

TOPIC = "lmnh"
MAX_MESSAGE_COUNT = 100000
MAX_NO_MESSAGE_COUNT = 60
TIMEOUT = 1
EARLIEST_KIOSK_TIME, LATEST_KIOSK_TIME = time(8, 45), time(18, 15)
BULK_SIZE = 100


def handle_cl_args() -> str:
    '''Handles command line argument for the logging format'''
    parser = argparse.ArgumentParser()
    parser.add_argument('log', default='console',
                        help='log to file or to console')
    args = parser.parse_args()

    log = args.log
    if log not in ['file', 'console']:
        raise RuntimeError('Invalid logging type given')
    return log


def choose_logging(log):
    '''Uses log argument to define the config for logging for the module'''
    if log == 'file':
        logging.basicConfig(filename="invalid_messages.log", encoding="utf-8", filemode="a",
                            format="{levelname} - {message}", style="{")
    else:
        logging.basicConfig(format="{levelname} - {message}", style="{")


def consume_message(consumer: Consumer) -> None:
    '''Consumes message from lmnh topic, 
    checks their validity and logs invalid messages separately
    Every 100 valid messages are processed and uploaded in bulk'''
    count = 0
    no_msg_count = 0
    buffer = []

    while count < MAX_MESSAGE_COUNT and no_msg_count < MAX_NO_MESSAGE_COUNT:
        try:
            msg = consumer.poll(TIMEOUT)

            if not msg:
                no_msg_count += 1

            if msg:
                no_msg_count = 0
                count += 1

                value = json.loads(msg.value().decode("utf-8"))
                validity_checks = [check_msg_validity(value, 'at'),
                                   check_msg_validity(value, 'site'),
                                   check_msg_validity(value, 'val')]
                if value.get('val') == -1:
                    validity_checks.append(check_msg_validity(value, 'type'))

                for invalid in validity_checks:
                    if invalid:  # invalid messages
                        logging.error(
                            f"[{count}] Offset: {msg.offset()}, Value: {value} - {invalid}")

                if all(x is None for x in validity_checks):  # valid messages
                    print(f"[{count}] Offset: {msg.offset()}, Value: {value}")
                    buffer.append(value)
                    if len(buffer) >= BULK_SIZE:
                        process_and_upload(buffer)
                        buffer.clear()

        except Exception as e:
            print(f"Unexpected error: {e}")

    consumer.close()
    print("Consumer closed.")


def check_msg_validity(message: dict, key: str) -> str | None:
    '''Checks value of the specified key - at, site, val, or type
    Invalid = missing, wrong type, out of range - returns a string
    Valid - returns None'''
    invalid = None  # default
    value = message.get(key)

    # Check if missing
    if value is None:
        invalid = f"INVALID: missing '{key}' key"
        return invalid

    # Check timestamp is within valid range
    if key == 'at':
        if not is_time_valid(value):
            invalid = "INVALID: 'at' is outside the valid time range"
        return invalid

    # Checks site, val or type can be converted to integers
    try:
        value = int(value)
    except:
        invalid = f"INVALID: '{key}' must be an integer"
        return invalid

    # Checks site, val or type are within valid range
    range_dict = {'site': {'min': 0, 'max': 5},
                  'val': {'min': -1, 'max': 4},
                  'type': {'min': 0, 'max': 1}}
    if int(value) < range_dict[key]['min'] or int(value) > range_dict[key]['max']:
        invalid = f"INVALID: '{key}' is out of range"

    return invalid


def is_time_valid(event_at: str) -> bool:
    '''Checks timestamp string is within the valid time range for the museum'''
    try:
        event_time = datetime.strptime(
            event_at[:19], "%Y-%m-%dT%H:%M:%S")
    except:
        return False
    if not EARLIEST_KIOSK_TIME <= event_time.time() <= LATEST_KIOSK_TIME:
        return False

    return True


def get_rds_connection() -> connection:
    '''Get connection'''

    conn = connect(
        user=environ["DATABASE_USERNAME"],
        password=environ["DATABASE_PASSWORD"],
        host=environ["DATABASE_IP"],
        port=environ["DATABASE_PORT"],
        database=environ["DATABASE_NAME"]
    )
    print('Connected to RDS')
    return conn


def get_max_id(table: str) -> int:
    '''Connects to RDS and returns the max ID/primary key from the specified table'''
    conn = get_rds_connection()

    with conn.cursor() as cursor:
        cursor.execute(f'SELECT {table}_id FROM {table};')
        ids = cursor.fetchall()

    conn.close()
    if len(ids) == 0:
        f'Max ID in {table} = -1'
        return -1
    max_id = max(ids)[0]
    print(f'Max ID in {table} = {max_id}')
    return max_id


def transform(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    '''Splits dataframe from kafka into two separate data frames:
    Requests are when val = -1, then type: 0, 1 is the useful data.
    Ratings are val: 0-4, so the type column is null and can be dropped.

    Columns and their positions are then matched to the postgres DB'''
    # REQUESTS
    request_df = df[df['val'] == -1]
    request_df = request_df.drop(columns=['val'])
    request_df = request_df.rename(columns={'at': 'event_at',
                                            'site': 'exhibition_id',
                                            'type': 'request_id'})
    # rearranging columns
    request_df = request_df.iloc[:, [1, 2, 0]]

    # choosing IDs
    request_interaction_id = get_max_id('request_interaction') + 1
    request_df.index = pd.Index(
        range(request_interaction_id, request_interaction_id + len(request_df)))

    request_df['exhibition_id'] = request_df['exhibition_id'].astype(int)
    request_df['request_id'] = request_df['request_id'].astype(int)
    print('request_interaction transformed')

    # RATINGS
    rating_df = df[df['val'] != -1]
    rating_df = rating_df.drop(columns=['type'])
    rating_df = rating_df.rename(columns={'at': 'event_at',
                                          'site': 'exhibition_id',
                                          'val': 'rating_id'})

    # rearranging columns
    rating_df = rating_df.iloc[:, [1, 2, 0]]

    # choosing IDs
    rating_interaction_id = get_max_id('rating_interaction') + 1
    rating_df.index = pd.Index(
        range(rating_interaction_id, rating_interaction_id + len(rating_df)))

    request_df['exhibition_id'] = request_df['exhibition_id'].astype(int)
    print('rating_interaction transformed')
    return request_df, rating_df


def upload_to_db(conn: connection, df: pd.DataFrame, table: str) -> pd.DataFrame | int:
    '''Uploads a pandas dataframe to a specified table in the database'''
    cursor = conn.cursor()

    # creating temporary csv file
    tmp_df = "./tmp_dataframe.csv"
    df.to_csv(tmp_df, index_label='id', header=False)
    with open(tmp_df, 'r', encoding="utf-8") as f:

        try:  # copy from temp file to table in database
            cursor.copy_from(f, table, sep=",")
            conn.commit()
        except DatabaseError as error:
            remove(tmp_df)
            logging.error(error)
            conn.rollback()
            cursor.close()
            return 1

    print('Copying to database complete')
    cursor.close()
    remove(tmp_df)
    return df


def process_and_upload(buffer: list[dict]):
    '''Takes buffer list of BULK_SIZE valid messages and creates a dataframe from it
    Then transforms and uploads data'''
    df = pd.DataFrame(buffer)
    print(f"Dataframe of {len(buffer)} valid messages created")
    request_df, rating_df = transform(df)

    rds_conn = get_rds_connection()

    upload_to_db(rds_conn, request_df, 'request_interaction')
    upload_to_db(rds_conn, rating_df, 'rating_interaction')

    rds_conn.close()
    print("Upload complete")


if __name__ == "__main__":
    log_to = handle_cl_args()
    choose_logging(log_to)

    load_dotenv()

    kafka_config = {
        'bootstrap.servers': environ["BOOTSTRAP_SERVERS"],
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': environ["USERNAME"],
        'sasl.password': environ["PASSWORD"],
        'group.id': environ["GROUP"],
        'auto.offset.reset': 'latest'  # every msg starting from now
    }

    # create a Consumer instance
    kafka_consumer = Consumer(kafka_config)

    # subscribe to topic
    kafka_consumer.subscribe([TOPIC])

    consume_message(kafka_consumer)
