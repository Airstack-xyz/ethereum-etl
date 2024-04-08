# MIT License
#
# Copyright (c) 2018 Evgeny Medvedev, evge.medvedev@gmail.com
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
import logging
import random
import os
import click
from prometheus_client import start_http_server
from blockchainetl.streaming.streaming_utils import configure_signals, configure_logging
from ethereumetl.enumeration.entity_type import EntityType

from ethereumetl.providers.auto import get_provider_from_uri
from ethereumetl.streaming.item_exporter_creator import create_item_exporters
from ethereumetl.thread_local_proxy import ThreadLocalProxy
from ethereumetl.constants import constants

@click.command(context_settings=dict(help_option_names=['-h', '--help']))
@click.option('-l', '--last-synced-block-file', default='last_synced_block.txt', show_default=True, type=str, help='')
@click.option('--lag', default=0, show_default=True, type=int, help='The number of blocks to lag behind the network.')
@click.option('-o', '--output', type=str,
              help='Either Google PubSub topic path e.g. projects/your-project/topics/crypto_ethereum; '
                   'or Postgres connection url e.g. postgresql+pg8000://postgres:admin@127.0.0.1:5432/ethereum; '
                   'or GCS bucket e.g. gs://your-bucket-name; '
                   'or kafka, output name and connection host:port e.g. kafka/127.0.0.1:9092 '
                   'or Kinesis, e.g. kinesis://your-data-stream-name'
                   'If not specified will print to console')
@click.option('-s', '--start-block', default=None, show_default=True, type=int, help='Start block')
@click.option('-E', '--end-block', default=None, show_default=True, type=int, help='End block')
@click.option('-e', '--entity-types', default=','.join(EntityType.ALL_FOR_INFURA), show_default=True, type=str,
              help='The list of entity types to export.')
@click.option('--period-seconds', default=10, show_default=True, type=int, help='How many seconds to sleep between syncs')
@click.option('-b', '--batch-size', default=10, show_default=True, type=int, help='How many blocks to batch in single request')
@click.option('-B', '--block-batch-size', default=1, show_default=True, type=int, help='How many blocks to batch in single sync round')
@click.option('-w', '--max-workers', default=5, show_default=True, type=int, help='The number of workers')
@click.option('--log-file', default=None, show_default=True, type=str, help='Log file')
@click.option('--pid-file', default=None, show_default=True, type=str, help='pid file')
@click.option('--mode', default='normal', show_default=True, type=str, help='mode to run the streamer ie. normal / correction')
@click.option('--blocks-to-reprocess', default=None, show_default=True, type=str, help='specify the blocks to reprocess in comma (,) separated format')
def stream(last_synced_block_file, lag, output, start_block, end_block, entity_types, max_workers,
           period_seconds=10, batch_size=2, block_batch_size=10, log_file=None, pid_file=None, mode = None, blocks_to_reprocess = None):
    """Streams all data types to console or Google Pub/Sub."""
    configure_logging(log_file)
    configure_signals()
    entity_types = parse_entity_types(entity_types)

    from ethereumetl.streaming.eth_streamer_adapter import EthStreamerAdapter
    from blockchainetl.streaming.streamer import Streamer

    if os.environ['BLOCKCHAIN'] == None:
        raise ValueError('BLOCKCHAIN env is missing')
    
    provider_uri = os.environ['PROVIDER_URI']
    if provider_uri == None:
        raise ValueError('PROVIDER_URI env is missing')
    
    if os.environ['KAFKA_BROKER_URI'] == None:
        raise ValueError('KAFKA_BROKER_URI env is missing')

    if mode == constants.RUN_MODE_CORRECTION:
        blocks_to_reprocess = [int(block) for block in blocks_to_reprocess.split(',')]
        logging.info('blocks_to_reprocess: {} with length: {}'.format(blocks_to_reprocess, len(blocks_to_reprocess)))
        if len(blocks_to_reprocess) == 0:
            raise ValueError('blocks_to_reprocess is empty')
    
    streamer_adapter = EthStreamerAdapter(
        batch_web3_provider=ThreadLocalProxy(lambda: get_provider_from_uri(provider_uri, batch=True)),
        item_exporter=create_item_exporters(output),
        batch_size=batch_size,
        max_workers=max_workers,
        entity_types=entity_types
    )
    streamer = Streamer(
        blockchain_streamer_adapter=streamer_adapter,
        last_synced_block_file=last_synced_block_file,
        lag=lag,
        start_block=start_block,
        end_block= end_block,
        period_seconds=period_seconds,
        block_batch_size=block_batch_size,
        pid_file=pid_file,
        mode=mode,
        blocks_to_reprocess=blocks_to_reprocess
    )
    
    port = int(os.environ.get('METRICS_PORT', constants.METRICS_PORT))
    start_http_server(port) # start prometheus server
    streamer.stream()


def parse_entity_types(entity_types):
    entity_types = [c.strip() for c in entity_types.split(',')]

    # validate passed types
    for entity_type in entity_types:
        if entity_type not in EntityType.ALL_FOR_STREAMING:
            raise click.BadOptionUsage(
                '--entity-type', '{} is not an available entity type. Supply a comma separated list of types from {}'
                    .format(entity_type, ','.join(EntityType.ALL_FOR_STREAMING)))

    return entity_types


def pick_random_provider_uri(provider_uri):
    provider_uris = [uri.strip() for uri in provider_uri.split(',')]
    return random.choice(provider_uris)
