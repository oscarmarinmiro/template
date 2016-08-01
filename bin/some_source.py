__author__ = '@oscarmarinmiro @ @outliers_es'

import pprint
import json
import re
import datetime
import rfc822

from elasticsearch import Elasticsearch, helpers


FILE_IN = ["../../assets/muestra/nlv.muestra.txt", "../../assets/muestra/sr.muestra.txt"]

# FILE_IN = ["../../../datasets/nolesvotes.flatId.sorted.json", "../../../datasets/spanishrevolution.flatId.sorted.json"]


DATASET_NAMES = ["nolesvotes", "spanish_revolution"]

HOST = "127.0.0.1"
PORT = 9200
INDEX_NAME = "tesis"
DOC_TYPE = "oscar"

DELETE = True

# Connect to elasticsearch

es = Elasticsearch([{'host': HOST, 'port': PORT}])

if DELETE:
    es.indices.delete(index = INDEX_NAME, ignore=[400, 404])

if not es.indices.exists(index=INDEX_NAME):
    es.indices.create(index=INDEX_NAME)

for i, input in enumerate(FILE_IN):
    with open(input, "rb") as file_in:

        dataset = DATASET_NAMES[i]

        print "==== Voy con el dataset %s" % dataset

        tweet_count = 0

        for line in file_in:
            line = line.rstrip()

            if tweet_count % 1000 == 0:
                print "Num tweets: %d" % tweet_count

            try:

                my_json = json.loads(line.split("\t")[1])

                date = my_json['created_at']

                # my_json['created_at_other'] = datetime.datetime.fromtimestamp(rfc822.mktime_tz(rfc822.parsedate_tz(date))).strftime("%YYYY-%m-%dT%H:%M:%SZ")

                my_json['created_at_other'] = date[:10] + "T" + date[11:] + 'Z'

                if my_json['retweet_count'] == "100+":
                    my_json['retweet_count'] = 200

                if 'retweeted_status' in my_json and my_json['retweeted_status']['retweet_count'] == "100+":
                    my_json['retweeted_status']['retweet_count'] = 200

                my_json['dataset'] = dataset
                es.index(index=INDEX_NAME, doc_type=DOC_TYPE, body=my_json)

                tweet_count += 1

            except Exception:
                print "Petando :" + pprint.pprint(Exception)


print "Num tweets: %d" % tweet_count


