# Webmon

Webmon is a website status poller that uses Kafka to move info about website
status to a PostgreSQL.  These are examples of things you can do with Aiven
services.

To run, you must have a producer and a consumer as part of the final solution
The following command line parameters allow you to select between the consumer and producer.
```
python main.py --consumer     OR
python main.py -c

python main.py --producer     OR
python main.py -p
```
## Requirements

Python 3.6+

## Background
Description:
The software module uses a kafka producer / consumer pair to determine the health of
a predetermined list of websites and inserts the status of the website into a postgres
database.
The producer module - polls some websites and then stores the results in a kafka
topic.
The consumer module - reads the kafka queue and inserts the message into the postgres table
*************************************************************************************
###Prerequisites:
```
aiven postgres sql server with a "defaultdb"
aiven kafka server with a topic "webmon" created
```
*************************************************************************************
### Aiven Services Setup
Step 1 - login to the aiven service and create a kafka topic
```avn user login
Username (email): douglas.kruger@skynetconsultingltd.com
douglas.kruger@skynetconsultingltd.com's Aiven password:
INFO	Aiven credentials written to: /Users/dkruger/.config/aiven/aiven-credentials.json
INFO	Default project set as 'skynetconsultingltd-9f71' (change with 'avn project switch <project>')

avn user info
USER                                    REAL_NAME       STATE   TOKEN_VALIDITY_BEGIN  PROJECTS                  AUTH
======================================  ==============  ======  ====================  ========================  ========
douglas.kruger@skynetconsultingltd.com  Douglas Kruger  active  null                  skynetconsultingltd-9f71  password
```
Step 2 - create the kafka topic
```avn service topic-create dkruger-kafka webmon --partitions 3 --replication 3
avn service topic-list dkruger-kafka
TOPIC_NAME PARTITIONS REPLICATION MIN_INSYNC_REPLICAS RETENTION_BYTES RETENTION_HOURS CLEANUP_POLICY TAGS
========== ========== =========== =================== =============== =============== ============== ====
webmon   3      3      1          -1        unlimited    delete
*************************************************************************************
```