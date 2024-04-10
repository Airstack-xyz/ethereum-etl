import os
from datetime import datetime
from ethereumetl.constants import constants

def filter_records(items):
    min_ts = get_minimum_ts(items) # get minimum timestamp from items list    
    
    ch_fallback_days = int(os.environ.get('CLICKHOUSE_FALLBACK_TS', constants.CLICKHOUSE_FALLBACK_TS))
    difference = datetime.utcnow() - datetime.utcfromtimestamp(min_ts)
    
    # check if timestamp is older than "CLICKHOUSE_FALLBACK_TS" days
    if difference.days > ch_fallback_days:
        items = filter_records_from_db(items, min_ts)
        return items, True # True represents that data was older than ch_fallback_days
        
    return items, False  # False represents that data was not older than ch_fallback_days

def filter_records_from_db(items, ts):
    queries = prepare_db_queries(items, ts)
    db_results = get_db_records(items, ts)
    
    # for each results <> with items and filter them
    # TODO: remove records from items that are present in db_records
    return items

def prepare_db_queries(items, ts):
    # filter messages based on "type"
    # use switch condition to identify what should be your query
    # prepare queries and return
    # TODO: prepare CH SQL queries
    # return { type: { items, queries } }
    pass

# items -> { type: { items, queries } }
def get_db_records(items, ts):
    # for each type[queries] -> run them on CH in parallel
    # return { type: { results } }
    # TODO: run CH SQL queries
    pass

def get_minimum_ts(items):
    # get timestamp of oldest message from items list
    record = min(items, key=lambda x: x.get("timestamp", float('inf')) if "timestamp" in x else x.get("block_timestamp", float('inf')))
    return record.get("timestamp") or record.get("block_timestamp")
