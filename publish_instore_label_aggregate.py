import base64
import sys
import datetime
import random

from google.cloud import pubsub_v1
import json

PROJECT_ID = "sej-dev-spanner-test"
TOPIC_NAME = "aggregation-by-label"


def _publish(project_id, topic_name, data):
    pub_client = pubsub_v1.PublisherClient()
    topic_path = pub_client.topic_path(project_id, topic_name)
    future = pub_client.publish(topic_path, data=data)
    # print('Published {} of message ID {}.'.format(data, future.result()))


if __name__ == "__main__":

    time_now = datetime.datetime.now()
    month = time_now.month
    year = time_now.year
    day = time_now.day
    hour = time_now.hour
    minute = time_now.minute
    second = time_now.second

    record_cnt = 952
    record_json_list = [] * record_cnt
    for i in range(record_cnt):
        record_json_list.append({
            "store_cd": "900"+ str(record_cnt).zfill(3),
            "created_datetime": str(year).zfill(4) + str(month).zfill(2) + str(day).zfill(2) + str(hour).zfill(2) + str(
                minute).zfill(2) + str(second).zfill(2),
            "instore_label_cd": str(i + 1).zfill(14),
            "item_cd": str(i).zfill(6),
            "tyunou_yyyymmdd": str(year).zfill(4) + str(month).zfill(2) + str(day).zfill(2),
            "shipping_no": "3",
            "delivery_amnt": 200,
            "sales_amnt": 5000,
            "disposal_amnt": 20,
            "returns_amnt": 6,
            "move_amnt": 10,
            "inside_store_amnt": 70,
            "delivery_plans_datetime": "20201012222600",
            "out_of_freshness_datetime": "20201012222600",
            "delivery_processing_datetime": "20201012222600",
            "final_shipment_datetime": "20201012222600",
            "final_shipment_processing_type": "12"
        })
    data_json = {
        "id": str(year).zfill(4) + str(month).zfill(2) + str(day).zfill(2) + str(hour).zfill(2) + str(minute).zfill(
        2) + str(second).zfill(2),
        "sendtimes": 1,
        "aggregate_unit": record_json_list
    }
    _publish(PROJECT_ID, TOPIC_NAME, json.dumps(data_json).encode("utf-8"))

    print('data_json: ' + json.dumps(data_json))
    data_b_body = base64.b64encode(json.dumps(data_json).encode('utf-8'))
    data_body = data_b_body.decode('ascii')
    data_json_message = {
        "messages": [
            {
                "attributes": {
                    "key": "iana.org/language_tag",
                    "value": "en"
                },
                "data": data_body
            }
        ]
    }
    data_pubsub_message = json.dumps(data_json_message).encode('utf-8')
    print('data pubsub message: ' + json.dumps(data_json))

