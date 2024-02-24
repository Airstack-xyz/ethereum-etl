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


import os
from blockchainetl.jobs.exporters.in_memory_item_exporter import InMemoryItemExporter
from ethereumetl.executors.batch_work_executor import BatchWorkExecutor
from blockchainetl.jobs.base_job import BaseJob
from ethereumetl.jobs.export_contracts_job import ExportContractsJob
from ethereumetl.jobs.exporters import contracts_item_exporter
from ethereumetl.mappers.token_mapper import EthTokenMapper
from ethereumetl.providers.auto import get_provider_from_uri
from ethereumetl.service.eth_token_service import EthTokenService
from ethereumetl.enumeration.entity_type import EntityType
from ethereumetl.json_rpc_requests import generate_get_code_json_rpc
import json
from ethereumetl.service.eth_contract_service import EthContractService
from ethereumetl.thread_local_proxy import ThreadLocalProxy


class ExportTokensJob(BaseJob):
    def __init__(self, web3, item_exporter, token_addresses_iterable, max_workers):
        self.item_exporter = item_exporter
        self.token_addresses_iterable = token_addresses_iterable
        self.batch_work_executor = BatchWorkExecutor(1, max_workers, EntityType.TOKEN)

        self.token_service = EthTokenService(web3, clean_user_provided_content)
        self.token_mapper = EthTokenMapper()

    def _start(self):
        self.item_exporter.open()

    def _export(self):
        self.batch_work_executor.execute(self.token_addresses_iterable, self._export_tokens)

    def _export_tokens(self, token_addresses):
        for token_address in token_addresses:
            self._export_token(token_address)

    def _export_token(self, token_address, block_number=None, token_type=None, is_proxy=False):
        token = self.token_service.get_token(token_address)
        token.block_number = block_number
        token.token_type = token_type
        token.is_proxy = is_proxy
        if is_proxy:
            token.implementation_address = self.token_service.get_proxy_implementation(token_address)
            if token.implementation_address is not None:
                exporter = InMemoryItemExporter(item_types=['contract'])
                job = ExportContractsJob(
                    contract_addresses_iterable=[token.implementation_address],
                    batch_size=1,
                    batch_web3_provider=ThreadLocalProxy(lambda: get_provider_from_uri(os.environ['PROVIDER_URI'], batch=True)),
                    item_exporter= exporter,
                    max_workers=1)
                job.run()
                contracts = exporter.get_items('contract')
                if len(contracts) > 0:
                    contract = contracts[0]
                    token_type = None
                    if contract.get('is_erc20'):
                        token_type = 'ERC20'
                    elif contract.get('is_erc721'):
                        token_type = 'ERC721'
                    elif contract.get('is_erc1155'):
                        token_type = 'ERC1155'
                    elif contract.get('is_proxy'):
                        token_type = 'PROXY'   # marking it as proxy if the implementation is a proxy ideally this should not be case.
                    token.token_type = token_type

        token_dict = self.token_mapper.token_to_dict(token)
        self.item_exporter.export_item(token_dict)

    def _end(self):
        self.batch_work_executor.shutdown()
        self.item_exporter.close()


ASCII_0 = 0


def clean_user_provided_content(content):
    if isinstance(content, str):
        # This prevents this error in BigQuery
        # Error while reading data, error message: Error detected while parsing row starting at position: 9999.
        # Error: Bad character (ASCII 0) encountered.
        return content.translate({ASCII_0: None})
    else:
        return content
