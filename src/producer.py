# *************************************************************************************
# Copyright (c) 2021 Skynet Consulting Ltd. http://www.skynetconsultingltd.com
# *************************************************************************************
# Description:
# The software module uses a kafka producer / consumer pair to determine the health of
# a predetermined list of websites and inserts the status of the website into a postgres
# database.
# This is the producer module - polls some websites and then stores the results in a kafka
# topic
# *************************************************************************************
import requests
import re
import time
import json
import kafka
import threading
import ssl

# Environment Setup - Kafka and Postgres - will be different for each customer or user
# For improvements, this can be in a config file or command line option
bootstrap_server = "dkruger-kafka-skynetconsultingltd-9f71.aivencloud.com:26676"
poll_interval = 15
re_response_match = "200"
kafka_topic = "webmon"
web_cafile = "C:/Users/douglas/Desktop/aiven/ca.pem"
web_certfile = "C:/Users/douglas/Desktop/aiven/service.cert"
web_keyfile = "C:/Users/douglas/Desktop/aiven/service.key"
verbose_f = True  # In production, set to False


# Function definitions
def verbose(msg):
    # For Debugging or visualization
    if verbose_f:
        print(msg)


# Polling routine to get the website status and returns the results
def poll_site(url, regex):
    try:
        resp = requests.get(url)
        match = regex and re.search(regex, resp.text)
        return {
            'url': url,
            'status': resp.status_code,
            'response_time': resp.elapsed.total_seconds(),
            'match': match and match.group(0),
        }
    # For the return status, selected codes from https://httpstatuses.com/
    # and use the response time of -1 and no regex match for exceptions
    except requests.exceptions.HTTPError as errh:
        print("Error HTTP:", errh)
        return {'url': url, 'status': '400', 'response_time': '-1', 'match': 'None', }
    except requests.exceptions.SSLError as errs:
        print("Error SSL:", errs)
        return {'url': url, 'status': '401', 'response_time': '-1', 'match': 'None', }
    except requests.exceptions.ConnectionError as errc:
        print("Error Connecting:", errc)
        return {'url': url, 'status': '444', 'response_time': '-1', 'match': 'None', }
    except requests.exceptions.Timeout as errt:
        print("Timeout Error:", errt)
        return {'url': url, 'status': '408', 'response_time': '-1', 'match': 'None', }
    except requests.exceptions.TooManyRedirects as errtm:
        print("Error TooManyRedirects:", errtm)
        return {'url': url, 'status': '303', 'response_time': '-1', 'match': 'None', }
    except requests.exceptions.RequestException as err:
        print("Error RequestException:", err)
        return {'url': url, 'status': '400', 'response_time': '-1', 'match': 'None', }
    except Exception as ex:
        template = "Fatal Exception of type {0} occurred. Arguments:\n{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        print(message)
        return {'url': url, 'status': '400', 'response_time': '-1', 'match': 'None', }


# Sends the kafka message
def send_message(producer, topic, message):
    # Topic must exist for producer to succeed
    verbose(message)
    producer.send(topic, json.dumps(message).encode('utf-8'))
    producer.flush()


# Driver loop - polls website and the publishes the status to kafka
def loop(producer, topic, url, regex=re_response_match, interval=poll_interval):
    while True:
        send_message(producer, topic, poll_site(url, regex))
        time.sleep(interval)


# Main Python Web Producer Routine
def web_producer():
    # Setup the Kafka Securit
    try:
        ssl_context = ssl.create_default_context(
            purpose=ssl.Purpose.CLIENT_AUTH,
            cafile=web_cafile,
        )
        ssl_context.load_cert_chain(
            certfile=web_certfile,
            keyfile=web_keyfile,
        )

        producer = kafka.KafkaProducer(
            bootstrap_servers=[bootstrap_server],
            security_protocol="SSL",
            ssl_context=ssl_context,
            max_block_ms=10000,
        )

        sites = [
            "https://blah12345.com",
            "https://httpstat.us/200",
            "https://httpstat.us/404",
            "https://httpstat.us/301",
            "https://httpstat.us/201?sleep=120000",
            "https://tls-v1-2.badssl.com:1012/",
            "https://expired.badssl.com/",
        ]

        # Create a separate thread for each website
        # Threads are safe for Kafka producers but not consumers
        for url in sites:
            threading.Thread(target=loop, args=(producer, kafka_topic, url)).start()

    except FileNotFoundError as err:
        print("Fatal Exception - SSL Security File(s) Not Found:", err)
    except Exception as ex:
        template = "Fatal Exception of type {0} occurred. Arguments:\n{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        print(message)
        raise SystemExit(ex)
