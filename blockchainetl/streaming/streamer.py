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
import os
import time
from datetime import datetime

from blockchainetl.streaming.streamer_adapter_stub import StreamerAdapterStub
from blockchainetl.file_utils import smart_open
from ethereumetl.constants import constants

from ethereumetl.metrics.prometheus import PrometheusConnector

class Streamer:
    def __init__(
            self,
            blockchain_streamer_adapter=StreamerAdapterStub(),
            last_synced_block_file='last_synced_block.txt',
            lag=0,
            start_block=None,
            end_block=None,
            period_seconds=10,
            block_batch_size=10,
            retry_errors=True,
            pid_file=None,
            mode=constants.RUN_MODE_NORMAL,
            blocks_to_reprocess= None):
        self.blockchain_streamer_adapter = blockchain_streamer_adapter
        self.last_synced_block_file = last_synced_block_file
        self.lag = lag
        self.start_block = start_block
        self.end_block = end_block
        self.period_seconds = period_seconds
        self.block_batch_size = block_batch_size
        self.retry_errors = retry_errors
        self.pid_file = pid_file
        self.mode = mode
        self.blocks_to_reprocess = blocks_to_reprocess
        self.last_synced_block = None
        
        # init prometheus client
        self.prometheus_client = PrometheusConnector()
        
        if self.mode == constants.RUN_MODE_NORMAL:
            # if "start-block" is provided and "last_synced_block_file" is not present
            # then write "start-block - 1" to "last_synced_block_file"
            if (self.start_block is not None) and (not os.path.isfile(self.last_synced_block_file)):
                write_last_synced_block(self.last_synced_block_file, self.start_block - 1)

            self.last_synced_block = read_last_synced_block(self.last_synced_block_file)

    def stream(self):
        try:
            if self.pid_file is not None:
                logging.info('Creating pid file {}'.format(self.pid_file))
                write_to_file(self.pid_file, str(os.getpid()))
            self.blockchain_streamer_adapter.open()
            if self.mode == constants.RUN_MODE_CORRECTION:
                logging.info('Running in correction mode')
                self._do_stream_correction()
            elif self.mode == constants.RUN_MODE_NORMAL:
                logging.info('Running in normal mode')
                self._do_stream()
    
        finally:
            self.blockchain_streamer_adapter.close()
            if self.pid_file is not None:
                logging.info('Deleting pid file {}'.format(self.pid_file))
                delete_file(self.pid_file)

    def _do_stream_correction(self):
        for block in self.blocks_to_reprocess:
            while True:
                try:
                    current_time = datetime.now()
                    self._sync_cycle_correction(block)
                    elapsed_time = datetime.now() - current_time
                    logging.info('Correction-> Synced block {} in {}'.format(block,elapsed_time))
                    break
                except Exception as e:
                    logging.exception('Correction-> An exception occurred while syncing block number: {} , is retryable erro: {}', block, e, self.retry_errors)
                    logging.info('Correction-> Sleeping for {} seconds...'.format(self.period_seconds))
                    time.sleep(self.period_seconds)

    def _do_stream(self):
        logging.info('End Block={} LastSyncedBlock={}'.format(self.end_block, self.last_synced_block))
        while (self.end_block is None or self.last_synced_block < self.end_block):
            synced_blocks = 0
            try:
                current_time = datetime.now()
                synced_blocks = self._sync_cycle()
                elapsed_time = datetime.now() - current_time
                logging.info('Synced {} blocks in {}'.format(synced_blocks, elapsed_time))
            except Exception as e:
                logging.exception('An exception occurred while syncing block data.', e)
                if not self.retry_errors:
                    raise e

            if synced_blocks <= 0:
                logging.info('Nothing to sync. Sleeping for {} seconds...'.format(self.period_seconds))
                time.sleep(self.period_seconds)

    def _sync_cycle_correction(self, target_block):
        logging.info('Correction-> target block {}, last synced block {}'.format(target_block, self.last_synced_block))
        self.blockchain_streamer_adapter.export_all(target_block, target_block)
        self.last_synced_block = target_block
    
    def _sync_cycle(self):
        current_block = self.blockchain_streamer_adapter.get_current_block_number()
        target_block = self._calculate_target_block(current_block, self.last_synced_block)
        blocks_to_sync = max(target_block - self.last_synced_block, 0)

        logging.info('Current block {}, target block {}, last synced block {}, blocks to sync {}'.format(
            current_block, target_block, self.last_synced_block, blocks_to_sync))
        
        self.prometheus_client.current_block.set(current_block)
        self.prometheus_client.target_block.set(target_block)
        self.prometheus_client.last_synced_block.set(self.last_synced_block)
        self.prometheus_client.blocks_to_sync.set(blocks_to_sync)

        if blocks_to_sync != 0:
            self.blockchain_streamer_adapter.export_all(self.last_synced_block + 1, target_block)
            logging.info('Writing last synced block {}'.format(target_block))
            write_last_synced_block(self.last_synced_block_file, target_block)
            self.last_synced_block = target_block

        return blocks_to_sync

    def _calculate_target_block(self, current_block, last_synced_block):
        target_block = current_block - self.lag
        target_block = min(target_block, last_synced_block + self.block_batch_size)
        target_block = min(target_block, self.end_block) if self.end_block is not None else target_block
        return target_block


def delete_file(file):
    try:
        os.remove(file)
    except OSError:
        pass


def write_last_synced_block(file, last_synced_block):
    write_to_file(file, str(last_synced_block) + '\n')


def read_last_synced_block(file):
    with smart_open(file, 'r') as last_synced_block_file:
        return int(last_synced_block_file.read())


def write_to_file(file, content):
    with smart_open(file, 'w') as file_handle:
        file_handle.write(content)
