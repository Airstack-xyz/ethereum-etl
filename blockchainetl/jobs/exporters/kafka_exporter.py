import collections
import json
import logging
import socket
import os

from kafka import KafkaProducer

from blockchainetl.jobs.exporters.converters.composite_item_converter import CompositeItemConverter
from ethereumetl.deduplication.redis import RedisConnector
from ethereumetl.utils import convert_numeric_to_string


class KafkaItemExporter:

    def __init__(self, item_type_to_topic_mapping, converters=()):
        self.item_type_to_topic_mapping = item_type_to_topic_mapping
        self.converter = CompositeItemConverter(converters)
        self.enable_deduplication = (os.environ.get('ENABLE_DEDUPLICATION') != None)

        self.connection_url = self.get_connection_url()
        self.producer = KafkaProducer(
            bootstrap_servers=self.connection_url,
            security_protocol='SASL_SSL',
            sasl_mechanism='SCRAM-SHA-512',
            sasl_plain_username=os.getenv('KAFKA_SCRAM_USERID'),
            sasl_plain_password=os.getenv('KAFKA_SCRAM_PASSWORD'),
            client_id=socket.gethostname(),
            compression_type=os.environ.get('KAFKA_COMPRESSION', 'lz4'),
            request_timeout_ms=60000,
            max_block_ms=120000,
            buffer_memory=100000000,
            retries=5,
            batch_size=32768,
            linger_ms=1)

        # use redis for deduplication of live messages  
        self.redis = None
        if self.enable_deduplication:
            self.redis = RedisConnector()

    def get_connection_url(self):
        kafka_broker_uri = os.environ['KAFKA_BROKER_URI']
        if kafka_broker_uri is None:
            raise Exception('KAFKA_BROKER_URI is not set')
        return kafka_broker_uri.split(',')

    def open(self):
        pass

    def export_items(self, items):
        futures = []
        for item in items:
            futures.append(self.export_item(item))

        # wait for all messages to be sent
        for future in futures:
            try:
                future.get(timeout=10)
            except Exception as e:
                logging.error(f'Failed to send message: {e}')
                raise e

    def export_item(self, item):
        item_type = item.get('type')
        item_id = item.get('id')

        if item_type == 'receipt':  # todo: fix this workaround
            item_id = f'receipt_{item.get("transaction_hash")}'

        if ((item_id is None) or (item_type is None) or (item_type not in self.item_type_to_topic_mapping)):
            logging.warning('Topic for item type "{}" is not configured.'.format(item_type))
            return

        item_type = self.item_type_to_topic_mapping[item_type]
        data = self.parse_data(item)

        if self.enable_deduplication:
            if not self.already_processed(item_type, item_id):
                # logging.info(f'Processing message of Type=[{item_type}]; Id=[{item_id}]')
                output = self.produce_message(item_type, data)
                self.mark_processed(item_type, item_id)
                return output
            logging.info(f'Message was already processed skipping...  Type=[{item_type}]; Id=[{item_id}]')
        else:
            return self.produce_message(item_type, data)

    def convert_items(self, items):
        for item in items:
            yield self.converter.convert_item(item)

    def close(self):
        if self.redis != None:
            self.redis.close()
        pass

    # utility function to set message as processed in Redis
    def mark_processed(self, item_type, item_id):
        if self.redis != None:
            return self.redis.add_to_set(item_type, item_id)
        return False

    # utility functions to check message was already processed or not
    def already_processed(self, item_type, item_id):
        if self.redis != None:
            return self.redis.exists_in_set(item_type, item_id)
        return False

    # utility functions to produce message to kafka
    def produce_message(self, item_type, data):
        return self.producer.send(item_type, value=data)

    # utility functions to convert numeric data to string format
    def parse_data(self, item):
        data = convert_numeric_to_string(item)
        return json.dumps(data).encode('utf-8')


def group_by_item_type(items):
    result = collections.defaultdict(list)
    for item in items:
        result[item.get('type')].append(item)

    return result
