import collections
import json
import logging
import socket
import os

from kafka import KafkaProducer

from blockchainetl.jobs.exporters.converters.composite_item_converter import CompositeItemConverter



class KafkaItemExporter:

    def __init__(self, item_type_to_topic_mapping, converters=()):
        self.item_type_to_topic_mapping = item_type_to_topic_mapping
        self.converter = CompositeItemConverter(converters)
        self.connection_url = self.get_connection_url()
        print(self.connection_url)
        self.producer = KafkaProducer(
            bootstrap_servers=self.connection_url,
            security_protocol='SASL_SSL',
            sasl_mechanism='SCRAM-SHA-512',
            sasl_plain_username=os.getenv('KAFKA_SCRAM_USERID'),
            sasl_plain_password=os.getenv('KAFKA_SCRAM_PASSWORD'),
            client_id=socket.gethostname(),
            compression_type='lz4',
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
        if item_type is not None and item_type in self.item_type_to_topic_mapping:
            data = json.dumps(item).encode('utf-8')
            # logging.debug(data)
            return self.producer.send(self.item_type_to_topic_mapping[item_type], value=data)
        else:
            logging.warning('Topic for item type "{}" is not configured.'.format(item_type))

    def convert_items(self, items):
        for item in items:
            yield self.converter.convert_item(item)

    def close(self):
        pass


def group_by_item_type(items):
    result = collections.defaultdict(list)
    for item in items:
        result[item.get('type')].append(item)

    return result
