import base64
import sys
import datetime
import random

from google.cloud import pubsub_v1
import json

PROJECT_ID = "sej-dev-spanner-test"
TOPIC_NAME = "liu-test"


def _publish(project_id, topic_name, data):
    pub_client = pubsub_v1.PublisherClient()
    topic_path = pub_client.topic_path(project_id, topic_name)
    future = pub_client.publish(topic_path, data=data)
    # print('Published {} of message ID {}.'.format(data, future.result()))


if __name__ == "__main__":

    time_now = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=9)))
    day = time_now.day
    hour = time_now.hour
    minute = time_now.minute
    second = time_now.second
    store_no = "000017"
    item_no = random.randint(1, 100)
    stock_record_cnt = 7
    stock_item_json_list = [] * stock_record_cnt
    for i in range(stock_record_cnt):
        stock_item_json_list.append({
            "store_cd": store_no,
            "created_datetime": "202007" + str(day).zfill(2) + str(hour).zfill(2) + str(minute).zfill(2) + str(
                second).zfill(2),
            "item_cd": str(i + 1).zfill(6),
            "stock_amnt": random.randint(0, 10),
            "item_handle_flag": "0",
            "non_handle_focus_item": "1",
            "mark_down_flag": "0",
            "item_handle_flag_2": "1",
            "stock_date": 0,
        })
    stock_json = {
        "id": "202007" + str(day).zfill(2) + str(hour).zfill(2) + str(minute).zfill(2) + str(second).zfill(2),
        "sendtimes": 1,
        "zaiko": stock_item_json_list
    }
    _publish(PROJECT_ID, TOPIC_NAME, json.dumps(stock_json).encode("utf-8"))

    print('stock_json: ' + json.dumps(stock_json))
    # stock_b_body = base64.b64encode(json.dumps(stock_json).encode('utf-8'))
    # stock_body = stock_b_body.decode('ascii')
    # stock_json_message = {
    #     "messages": [
    #         {
    #             "attributes": {
    #                 "key": "iana.org/language_tag",
    #                 "value": "en"
    #             },
    #             "data": stock_body
    #         }
    #     ]
    # }
    # stock_pubsub_message = json.dumps(stock_json_message).encode('utf-8')
    print('stock pubsub message: ' + json.dumps(stock_json))

