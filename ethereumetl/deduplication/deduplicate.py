import os
from datetime import datetime
from ethereumetl.constants import constants

def deduplicate_records(records, db):
    ch_fallback_days = int(os.environ.get('CLICKHOUSE_FALLBACK_TS', constants.CLICKHOUSE_FALLBACK_TS))
    
    for record in records:
        record = tuple(record)
        items = record[0]
        ts_key = record[1]
        
        min_ts = get_minimum_ts(items, ts_key)
        if is_ts_older(min_ts, ch_fallback_days):
            items = filter_records(items, ts_key, min_ts, db)

def is_ts_older(ts, days):
    difference = datetime.utcnow() - datetime.utcfromtimestamp(ts)
    return difference.days > days    

def filter_records(items, ts_key, min_ts_epoch, db):
    if items == None or len(items) == 0:
        return items
    
    message_type = items[0].get('type')
    table_name = get_table_name(message_type)
    min_ts = datetime.utcfromtimestamp(min_ts_epoch).strftime('%Y-%m-%d')
    
    # extract all ids
    ids = [obj["id"] for obj in items]
    
    query = f'SELECT id FROM {table_name} WHERE id IN ({ids}) AND {ts_key} >= {min_ts}'
    db_records = db.run_query(query)
    
    return [item for item in items if item['id'] not in db_records]

def get_table_name(message_type):
    blockchain = os.environ['BLOCKCHAIN']    
    return "degen.todo"
    
def get_minimum_ts(items, key):
    # get timestamp of oldest message from items list
    record = min(items, key=lambda x: x.get(key, float('inf')))
    return record.get(key)
