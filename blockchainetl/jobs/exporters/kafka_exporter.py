import collections
import json
import logging
import socket
import os

from kafka import KafkaProducer

from blockchainetl.jobs.exporters.converters.composite_item_converter import CompositeItemConverter
from ethereumetl.deduplication.redis import RedisConnector

class KafkaItemExporter:

    def __init__(self, item_type_to_topic_mapping, converters=()):
        self.item_type_to_topic_mapping = item_type_to_topic_mapping
        self.converter = CompositeItemConverter(converters)
        self.redis = RedisConnector()
           
        self.connection_url = self.get_connection_url()
        self.producer = KafkaProducer(
            bootstrap_servers=self.connection_url,
            security_protocol='SASL_SSL',
            sasl_mechanism='SCRAM-SHA-512',
            sasl_plain_username=os.getenv('KAFKA_SCRAM_USERID'),
            sasl_plain_password=os.getenv('KAFKA_SCRAM_PASSWORD'),
            client_id=socket.gethostname(),
            compression_type=os.environ.get('KAFKA_COMPRESSION', 'lz4'),
            request_timeout_ms= 60000,
            max_block_ms= 120000,
            buffer_memory= 100000000)

    def get_connection_url(self):
        kafka_broker_uri = os.environ['KAFKA_BROKER_URI']
        if kafka_broker_uri is None:
            raise Exception('KAFKA_BROKER_URI is not set')
        return kafka_broker_uri.split(',')
    
    def open(self):
        pass

    def export_items(self, items):        
        for item in items:
            self.export_item(item)

    def export_item(self, item):
        item_type = item.get('type')
        item_id = item.get('id')
        
        if item_id is not None and item_type is not None and item_type in self.item_type_to_topic_mapping:
            item_type = self.item_type_to_topic_mapping[item_type]
            data = json.dumps(item).encode('utf-8')
            
            if not self.already_processed(item_type, item_id):
                logging.info(f'Processing message of Type=[{item_type}]; Id=[{item_id}]')
                self.mark_processed(item_type, item_id)
                return self.producer.send(item_type, value=data)
                
            logging.info(f'Message was already processed skipping...  Type=[{item_type}]; Id=[{item_id}]')
        else:
            logging.warning('Topic for item type "{}" is not configured.'.format(item_type))

    def convert_items(self, items):
        for item in items:
            yield self.converter.convert_item(item)

    def close(self):
        self.redis.close()
        pass

    # utility function to set message as processed in Redis
    def mark_processed(self, item_type, item_id):
        return self.redis.add_to_set(item_type, item_id)

    # utility functions to check message was already processed or not
    def already_processed(self, item_type, item_id):
        return self.redis.exists_in_set(item_type, item_id)
    
def group_by_item_type(items):
    result = collections.defaultdict(list)
    for item in items:
        result[item.get('type')].append(item)

    return result
