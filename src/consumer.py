# *************************************************************************************
# Copyright (c) 2021 Skynet Consulting Ltd. http://www.skynetconsultingltd.com
# *************************************************************************************
# Description:
# The software module uses a kafka producer / consumer pair to determine the health of
# a predetermined list of websites and inserts the status of the website into a postgres
# database.
# This is the consumer module - polls kafka queue and inserts the records in postgres
# *************************************************************************************
import psycopg2
# import the error handling libraries for psycopg2
from psycopg2 import OperationalError
import kafka
import json
import ssl
import sys

# Environment Setup - Kafka and Postgres - will be different for each customer or user
# For improvements, this can be in a config file or command line option
postgres_con = "postgres://dkruger:vtyue32rv9P8lq2s@mypostgres-skynetconsultingltd-9f71.aivencloud.com:26674/webpage_health?sslmode=require"
bootstrap_server = "dkruger-kafka-skynetconsultingltd-9f71.aivencloud.com:26676"
kafka_topic = "webmon"
web_cafile = "C:/Users/douglas/Desktop/aiven/ca.pem"
web_certfile = "C:/Users/douglas/Desktop/aiven/service.cert"
web_keyfile = "C:/Users/douglas/Desktop/aiven/service.key"
verbose_f = True    # In production, set to False
debug_sql_f = True  # In production, set to False


# Function definitions
def debug_sql(sql):
    # For Debugging or visualization
    if debug_sql_f:
        print("Executing SQL: " + sql)


def verbose(msg):
    # For Debugging or visualization
    if verbose_f:
        print(msg)


def psycopg2_except(err):
    # Reference: https://kb.objectrocket.com/postgresql/python-error-handling-with-the-psycopg2-postgresql-adapter-645
    err_type, err_obj, traceback = sys.exc_info()

    # get the line number when exception occurred
    line_num = traceback.tb_lineno

    # print the connect() error
    print("\npsycopg2 ERROR:", err, "on line number:", line_num)
    print("psycopg2 traceback:", traceback, "-- type:", err_type)

    # psycopg2 extensions.Diagnostics object attribute
    print("\nextensions.Diagnostics:", err.diag)

    # print the pgcode and pgerror exceptions
    print("pgerror:", err.pgerror)
    print("pgcode:", err.pgcode, "\n")


def drop_table():
    try:
        sql_string = "DROP TABLE webmon_polls"
        debug_sql(sql_string)

        # Open a connection to the database and setup a cursor
        conn = psycopg2.connect(postgres_con)
        conn.cursor().execute(sql_string)
        # close communication with the PostgreSQL database server then commit the changes
        conn.cursor().close()
        conn.commit()
        conn.close()
    except OperationalError as err:
        # pass exception to function
        psycopg2_except(err)
    except Exception as err:
        # pass exception to function
        psycopg2_except(err)
        raise SystemExit(err)


def create_table():
    try:
        sql_string = "\
            CREATE TABLE IF NOT EXISTS webmon_polls (\
            url char(50),\
            status_code integer,\
            response_time float,\
            regex_match char(50))"
        debug_sql(sql_string)

        # Open a connection to the database and setup a cursor
        conn = psycopg2.connect(postgres_con)
        # Create the table
        conn.cursor().execute(sql_string)
        # close communication with the PostgreSQL database server then commit the changes
        conn.cursor().close()
        conn.commit()
        conn.close()
    except OperationalError as err:
        # pass exception to function
        psycopg2_except(err)
    except Exception as err:
        # pass exception to function
        psycopg2_except(err)
        raise SystemExit(err)


def insert_record(msg):
    try:
        # Get the data from the message
        url, status, response_time, match = map(msg.get, ['url', 'status', 'response_time', 'match'])
        verbose(f"""Retrieved url: {url}, status_code:{status}, response_time:{response_time}, regex_match:{match})""")
        debug_sql(f"""insert into webmon_polls (url, status_code, response_time, regex_match)
               values ('{url}', '{status}', '{response_time}', '{match}')""")

        # Open a connection to the database and setup a cursor
        conn = psycopg2.connect(postgres_con)
        # Use the following to avoid SQL injection into the variables
        sql_string = "insert into webmon_polls (url, status_code, response_time, regex_match) values (%s, %s, %s, %s)"
        conn.cursor().execute(sql_string, (url, status, response_time, match,))
        # close communication with the PostgreSQL database server then commit the changes
        conn.cursor().close()
        conn.commit()
        conn.close()
    except OperationalError as err:
        # pass exception to function
        psycopg2_except(err)
    except Exception as err:
        # pass exception to function
        psycopg2_except(err)
        raise SystemExit(err)


# Main Python Web Consumer Routine
def web_consumer():
    try:
        # Setup the Kafka Security
        ssl_context = ssl.create_default_context(
            purpose=ssl.Purpose.CLIENT_AUTH,
            cafile=web_cafile,
        )
        ssl_context.load_cert_chain(
            certfile=web_certfile,
            keyfile=web_keyfile,
        )

        # Setup the Kafka consumer
        consumer = kafka.KafkaConsumer(
            kafka_topic,
            bootstrap_servers=[bootstrap_server],
            security_protocol="SSL",
            ssl_context=ssl_context,
            value_deserializer=lambda bs: json.loads(bs.decode("utf-8")),
            auto_offset_reset="earliest",
            enable_auto_commit=True,
        )

        # Create the Postgres table if needed
        create_table()

        # Read the Kafka queue and process the data into Postgres then commit the Kafka message
        for msg in consumer:
            insert_record(msg.value)

    except FileNotFoundError as err:
        print("Fatal Exception - SSL Security File(s) Not Found:", err)
    except Exception as ex:
        template = "Fatal Exception of type {0} occurred. Arguments:\n{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        print(message)
        raise SystemExit(ex)
