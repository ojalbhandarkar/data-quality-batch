from datetime import datetime

from src.reader import read
from src.utils import get_current_time
import hashlib


def execute_rule_queries(entity, failed_records_query, total_records_query, context):
    failed_records_query_execution_start_time = get_current_time()
    failed_records = read(entity, failed_records_query, context)
    failed_records_query_execution_end_time = get_current_time()

    total_records_query_execution_start_time = get_current_time()
    total_records_count = read(entity, total_records_query, context)
    total_records_query_execution_end_time = get_current_time()
    primary_key = entity['primary_key']
    query_id=hashlib.sha256(failed_records_query.encode('utf-8')).hexdigest()

    return {'failed_records': failed_records,
            'total_records_count': total_records_count.first()['total_count'],
            'failed_records_query': failed_records_query,
            'total_records_query': total_records_query,
            'failed_records_query_execution_start_time': failed_records_query_execution_start_time,
            'failed_records_query_execution_end_time': failed_records_query_execution_end_time,
            'total_records_query_execution_start_time': total_records_query_execution_start_time,
            'total_records_query_execution_end_time': total_records_query_execution_end_time,
            'primary_key': primary_key,
            'query_execution_id': query_id
            }
