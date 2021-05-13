import base64
import sys
import datetime
import random
import json
import argparse

from google.cloud import pubsub_v1
import json

PROJECT_ID = "sej-dev-spanner-test"
TOPIC_NAME = "liu-test"


def _publish(project_id, topic_name, data):
    pub_client = pubsub_v1.PublisherClient()
    topic_path = pub_client.topic_path(project_id, topic_name)
    future = pub_client.publish(topic_path, data=data)
    # print('Published {} of message ID {}.'.format(data, future.result()))

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("file", default="store.csv", help="csv file of store_cd list", type=str)
    parser.add_argument("-project", default="sej-dev-spanner-test", help="your-project", type=str)
    parser.add_argument("-topic", default="instore-label-aggregate", help="your_topic", type=str)
    my_args = parser.parse_args()
    return my_args


if __name__ == "__main__":
    args = get_args()
    print(f"file={args.file}")
    print(f"project={args.project}")
    print(f"topic={args.topic}")
    json_file = open(args.file, 'r')
    json_object = json.load(json_file)
    print(json_object)
    _publish(args.project, args.topic, json.dumps(json_object).encode("utf-8"))

    # print('stock_json: ' + json.dumps(stock_json))
    #
    # print('stock pubsub message: ' + json.dumps(stock_json))

