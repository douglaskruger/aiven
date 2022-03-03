# *************************************************************************************
# Copyright (c) 2021 Skynet Consulting Ltd. http://www.skynetconsultingltd.com
# *************************************************************************************
# Description:
# The software module uses a kafka producer / consumer pair to determine the health of
# a predetermined list of websites and inserts the status of the website into a postgres
# database.
# This is the main python module - that will either call the producer or the consumer
# *************************************************************************************
import argparse
import sys
from consumer import web_consumer
from producer import web_producer


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--consumer', action='store_true', default=False, help="Web Monitor Producer")
    parser.add_argument('-p', '--producer', action='store_true', default=False, help="Web Monitor Consumer")
    args = parser.parse_args()
    validate_args(args)

    if args.producer:
        web_producer()
    elif args.consumer:
        web_consumer()


def validate_args(args):
    if args.producer and args.consumer:
        fail("--producer and --consumer are mutually exclusive")
    elif not args.producer and not args.consumer:
        fail("--producer or --consumer are required")


def fail(message):
    print(message, file=sys.stderr)
    exit(1)


if __name__ == '__main__':
    main()
