import os
import logging
import asyncio
from datetime import datetime

from ethereumetl.constants import constants
from ethereumetl.enumeration.entity_type import EntityType

def deduplicate_records(records, ts_key, db):
    ch_fallback_days = int(os.environ.get('CLICKHOUSE_FALLBACK_DAYS', constants.CLICKHOUSE_FALLBACK_DAYS))
    
    if records == None or len(records) == 0:
        return records
    
    min_ts = get_minimum_ts(records, ts_key)
    if is_ts_older(min_ts, ch_fallback_days):
        records = asyncio.run(filter_records(records, ts_key, min_ts, db))

def is_ts_older(ts, days):
    difference = datetime.utcnow() - datetime.utcfromtimestamp(ts)
    return difference.days > days    

async def filter_records(items, ts_key, min_ts_epoch, db):
    if items == None or len(items) == 0:
        return items
    
    message_type = items[0].get('type')
    skip_message_types = os.environ.get('CLICKHOUSE_SKIP_FOR_MESSAGE_TYPES')
    
    if skip_message_types != None and (message_type in skip_message_types.split(',')):
        logging.info(f'Ignoring check for deduplication for type {message_type} as it is ignored')
        return items
    
    table_name = get_table_name(message_type)
    if table_name == None:
        logging.warn(f'Ignoring check for deduplication for type {message_type} as table not found')
        return items
    
    min_ts = datetime.utcfromtimestamp(min_ts_epoch).strftime('%Y-%m-%d')
    
    # extract all ids
    ids = [obj["id"] for obj in items]
    
    parameters = { 'table': table_name, 'ids': ids, 'timestamp_key': ts_key, 'block_timestamp': min_ts }
    
    query = '''SELECT id FROM {table:Identifier} WHERE id IN {ids:Array(String)} and {timestamp_key:Identifier} >= {block_timestamp:String}'''
    
    db_results = await db.run_query(query, parameters)
    
    ids_from_db = [t[0] for t in db_results]    
    return [item for item in items if item['id'] not in ids_from_db]

def get_table_name(message_type):
    if message_type == EntityType.BLOCK:
        return 'blocks'
    elif message_type == EntityType.TRANSACTION:
        return 'transactions'
    elif message_type == EntityType.LOG:
        return 'logs'
    elif message_type == EntityType.TOKEN_TRANSFER:
        return 'token_transfers'
    elif message_type == EntityType.TRACE:
        return 'traces'
    elif message_type == EntityType.GETH_TRACE:
        return 'traces'
    elif message_type == EntityType.CONTRACT:
        return 'contracts'
    elif message_type == EntityType.TOKEN:
        return 'tokens'
    
def get_minimum_ts(items, key):
    # get timestamp of oldest message from items list
    record = min(items, key=lambda x: x.get(key, float('inf')))
    return record.get(key)
