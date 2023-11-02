import json
import string
from datetime import datetime
import random

from google.cloud.storage import Client, blob
from pyspark.sql import SparkSession


def get_spark_session():
    return SparkSession.builder \
        .appName("Data Quality").enableHiveSupport() \
        .getOrCreate()


def get_empty_data_frame(schema):
    return get_spark_session().createDataFrame(get_spark_session().sparkContext.emptyRDD(),
                                               schema=schema)


def get_unique_id():
    while True:
        number = str(''.join(random.choices(string.digits, k=8)))
        if not number.startswith('0'):
            return number


def read_file(file_path):
    keys = {}
    is_json_file = file_path.endswith('.json')
    is_properties_file = file_path.endswith('.properties')
    global data

    if file_path.startswith('gs://'):
        data = gs_reader(file_path)

        if is_json_file:
            data = json.loads(data)

        if is_properties_file:

            for line in data.splitlines():
                if '=' in line:
                    name, value = line.split('=', 1)
                    keys[name.strip()] = value.strip()
            data = keys
    else:
        if is_json_file:
            with open(file_path) as f:
                data = json.loads(f.read())

        if is_properties_file:
            with open(file_path) as f:
                for line in f:
                    if '=' in line:
                        name, value = line.split('=', 1)
                        keys[name.strip()] = value.strip()
                data = keys

    return data


def gs_reader(path):
    length_of_google_storage_prefix = 5
    bucket_name = path[length_of_google_storage_prefix:].split('/')[0]
    file_name = path[6 + len(bucket_name):]
    bucket = Client().get_bucket(bucket_name)
    return bucket.get_blob(file_name).download_as_text(encoding="utf-8")


def get_current_time():
    current_datetime = datetime.now()
    return current_datetime


def get_duration(end_time, start_time):
    delta = end_time - start_time
    return int(delta.total_seconds() * 1000)
