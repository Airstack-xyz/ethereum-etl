import os
import logging
import asyncio
from datetime import datetime
from ethereumetl.constants import constants

def deduplicate_records(records, ts_key, db):
    logging.info('Request to check for deduplication')
    ch_fallback_days = int(os.environ.get('CLICKHOUSE_FALLBACK_DAYS', constants.CLICKHOUSE_FALLBACK_DAYS))
    
    if records == None or len(records) == 0:
        return records
    
    min_ts = get_minimum_ts(records, ts_key)
    logging.info(f'Minimum ts to records to check for deduplication - {min_ts}')
    
    if is_ts_older(min_ts, ch_fallback_days):
        logging.info('Fetching details from Clickhouse for records')
        records = asyncio.run(filter_records(records, min_ts, db))
        logging.info('Fetched details from Clickhouse for records')
    return records

def is_ts_older(ts, days):
    difference = datetime.utcnow() - datetime.utcfromtimestamp(ts)
    return difference.days > days    

async def filter_records(items, min_ts_epoch, db):
    if items == None or len(items) == 0:
        return items
    
    message_type = items[0].get('type')
    skip_message_types = os.environ.get('CLICKHOUSE_SKIP_FOR_MESSAGE_TYPES')
    
    if skip_message_types != None and (message_type in skip_message_types.split(',')):
        logging.info(f'Ignoring check for deduplication for type {message_type} as it is ignored')
        return items
    
    table_name = get_table_name(message_type)
    ts_column_name = get_table_ts_column_name(message_type)
    if table_name == None:
        logging.warn(f'Ignoring check for deduplication for type {message_type} as table not found')
        return items
    
    min_ts = datetime.utcfromtimestamp(min_ts_epoch).strftime('%Y-%m-%d')
    
    # extract all ids
    ids = list([obj["id"] for obj in items])
    ids_from_db = []
    
    parameters = { 'table': table_name, 'ids': [], 'timestamp_key': ts_column_name, 'block_timestamp': min_ts }
    query = '''SELECT id FROM {table:Identifier} WHERE id IN {ids:Array(String)} and {timestamp_key:Identifier} >= {block_timestamp:String}'''
    
    chunk_size = int(os.environ.get('CLICKHOUSE_QUERY_CHUNK_SIZE', constants.CLICKHOUSE_QUERY_CHUNK_SIZE))
    for i in range(0, len(ids), chunk_size):
        chunk = ids[i:i + chunk_size]
        parameters['ids'] = chunk
        
        db_results = await db.run_query(query, parameters)
        ids_from_db = ids_from_db + [t[0] for t in db_results]
    
    return [item for item in items if item['id'] not in ids_from_db]

def get_table_name(message_type):
    return constants.ENTITY_TO_TABLE_MAP.get(message_type)

def get_table_ts_column_name(message_type):
    return constants.ENTITY_TO_TABLE_TS_COLUMNS_MAP.get(message_type)
    
def get_minimum_ts(items, key):
    # get timestamp of oldest message from items list
    record = min(items, key=lambda x: x.get(key, float('inf')))
    return record.get(key)
