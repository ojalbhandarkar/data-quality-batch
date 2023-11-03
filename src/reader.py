from src.utils import get_spark_session
import hashlib

def parquet(entity, query):
    path = [entity_property for entity_property in entity['properties']
                if entity_property['key'] == 'PATH'][0]['value']
    data = get_spark_session().read.parquet(path, header=True)
    data.registerTempTable(entity['entity_physical_name'])
    return get_spark_session().sql(query)


def csv(entity, query):
    path = [entity_property for entity_property in entity['properties']
            if entity_property['key'] == 'PATH'][0]['value']
    data = get_spark_session().read.csv(path, header=True)
    data.registerTempTable(entity['entity_physical_name'])
    return get_spark_session().sql(query)


def big_query(entity, query, context):
    get_spark_session().conf.set('temporaryGcsBucket', context.get_value('temp_gcs_bucket_name'))
    get_spark_session().conf.set('materializationDataset', context.get_value('bq_dataset'))
    query_id = hashlib.sha1(query.encode('utf-8')).hexdigest()
    get_spark_session().conf.set("bigQueryJobLabel.query_id", query_id)
    data = get_spark_session().read.format('bigquery'). \
        option('project', context.get_value('project_id')). \
        option('table', entity['entity_physical_name']). \
        load()

    data.registerTempTable(entity['entity_name'])
    print(f"query_id={query_id}")
    return get_spark_session().sql(query)


def hive(query):
    return get_spark_session().sql(query)


def read(entity, query, context):
    entity_sub_type = entity['entity_sub_type']
    data = None
    if entity_sub_type == 'CSV':
        data = csv(entity, query)
    if entity_sub_type == 'BIG_QUERY':
        data = big_query(entity, query, context)
    if entity_sub_type == 'HIVE':
        data = hive(query)
    return data
