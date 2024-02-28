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


from ast import List
import logging
from builtins import map

from ethereumetl.domain.token_transfer import EthTokenTransfer
from ethereumetl.utils import chunk_string, hex_to_dec, to_normalized_address, hex_strings_to_dec
from typing import List, Union
from ethereumetl.constants import constants

logger = logging.getLogger(__name__)

class EthTokenTransferExtractor(object):
    def extract_transfer_from_log(self, receipt_log):

        topics = receipt_log.topics
        if topics is None or len(topics) < 1:
            # This is normal, topics can be empty for anonymous events
            return None
        topic_0= topics[0].casefold()
        if topic_0 == constants.ERC20_ERC721_TRANSFER_EVENT_TOPIC:
            token_transfer = EthTokenTransfer()
            topics_length = len(topics)
            if topics_length == 3:
                token_transfer.token_type = constants.TOKEN_TYPE_ERC20
            elif topics_length == 4:
                token_transfer.token_type = constants.TOKEN_TYPE_ERC721
            token_transfer.category = constants.SINGLE_TRANSFER
            # Handle unindexed event fields
            topics_with_data = topics + split_to_words(receipt_log.data)
            # if the number of topics and fields in data part != 4, then it's a weird event
            if len(topics_with_data) != 4:
                logger.warning("The number of topics and data parts is not equal to 4 in log {} of transaction {}"
                               .format(receipt_log.log_index, receipt_log.transaction_hash))
                return None

            token_transfer.token_address = to_normalized_address(receipt_log.address)
            token_transfer.from_address = word_to_address(topics_with_data[1])
            token_transfer.to_address = word_to_address(topics_with_data[2])
            token_transfer.value = hex_to_dec(topics_with_data[3])
            if token_transfer.token_type == constants.TOKEN_TYPE_ERC721:
                token_transfer.value = 1
                token_transfer.token_id = hex_to_dec(topics_with_data[3])
           
            token_transfer.transaction_hash = receipt_log.transaction_hash
            token_transfer.log_index = receipt_log.log_index
            token_transfer.block_number = receipt_log.block_number
            token_transfer.transaction_index = receipt_log.transaction_index
            token_transfer.operator = receipt_log.tx_from
            token_transfer.transfer_type = get_transfer_type(token_transfer.from_address, token_transfer.to_address)
            return token_transfer
        
        elif topic_0 == constants.ERC1155_TRANSFER_SINGLE_EVENT_TOPIC:
            # Handle unindexed event fields
            topics_with_data = topics + split_to_words(receipt_log.data)
            # if the number of topics and fields in data part != 6, then it's a weird event
            if len(topics_with_data) != 6:
                logger.warning("The number of topics and data parts is not equal to 6 in log {} of transaction {}"
                                .format(receipt_log.log_index, receipt_log.transaction_hash))
                return None

            token_transfer = EthTokenTransfer()
            token_transfer.token_address = to_normalized_address(receipt_log.address)
            token_transfer.operator = word_to_address(topics_with_data[1])
            token_transfer.from_address = word_to_address(topics_with_data[2])
            token_transfer.to_address = word_to_address(topics_with_data[3])
            token_transfer.token_id = hex_to_dec(topics_with_data[4])
            token_transfer.value = hex_to_dec(topics_with_data[5])
            token_transfer.transaction_hash = receipt_log.transaction_hash
            token_transfer.log_index = receipt_log.log_index
            token_transfer.block_number = receipt_log.block_number
            token_transfer.transaction_index = receipt_log.transaction_index
            token_transfer.category = constants.SINGLE_TRANSFER
            token_transfer.token_type = constants.TOKEN_TYPE_ERC1155
            token_transfer.transfer_type = get_transfer_type( token_transfer.from_address, token_transfer.to_address)
            return token_transfer
        
        elif topic_0 == constants.ERC1155_TRANSFER_BATCH_EVENT_TOPIC:
            decoded_data = trim_log_data_token_ids_and_values(receipt_log.data)
            token_transfer = EthTokenTransfer()
            token_transfer.token_address = to_normalized_address(receipt_log.address)
            token_transfer.operator = word_to_address(topics[1])
            token_transfer.from_address = word_to_address(topics[2])
            token_transfer.to_address = word_to_address(topics[3])
            token_transfer.token_ids = hex_strings_to_dec(decoded_data[0])
            token_transfer.values = hex_strings_to_dec(decoded_data[1])
            token_transfer.transaction_hash = receipt_log.transaction_hash
            token_transfer.log_index = receipt_log.log_index
            token_transfer.block_number = receipt_log.block_number
            token_transfer.transaction_index = receipt_log.transaction_index
            token_transfer.category = constants.BATCH_TRANSFER
            token_transfer.token_type = constants.TOKEN_TYPE_ERC1155
            token_transfer.transfer_type = get_transfer_type( token_transfer.from_address, token_transfer.to_address)
            return token_transfer

        return None

def get_transfer_type(from_address, to_address):
    if from_address == constants.ZERO_ADDRESS and to_address not in constants.BURN_ADDRESSES:
        return constants.TRANSFER_TYPE_MINT
    elif from_address not in constants.BURN_ADDRESSES and to_address not in constants.BURN_ADDRESSES:
        return constants.TRANSFER_TYPE_TRANSFER
    elif to_address in constants.BURN_ADDRESSES:
        return constants.TRANSFER_TYPE_BURN

def split_to_words(data):
    if data and len(data) > 2:
        data_without_0x = data[2:]
        words = list(chunk_string(data_without_0x, 64))
        words_with_0x = list(map(lambda word: '0x' + word, words))
        return words_with_0x
    return []


def word_to_address(param):
    if param is None:
        return None
    elif len(param) >= 40:
        return to_normalized_address('0x' + param[-40:])
    else:
        return to_normalized_address(param)

def trim_log_data_token_ids_and_values(s: str) -> (List[str], List[str], Exception):
    # Remove the "0x" prefix
    s = s[2:]
    
    ids = []
    values = []
    data = s[128:]  # Skip the first 130 characters
    i = 0
    
    # Function to extract the next 64 characters and convert to an integer
    def next_int():
        nonlocal i
        if i + 64 > len(data):
            raise Exception("insufficient data")
        param_hex = data[i:i + 64]
        i += 64
        return int(param_hex, 16)
    
    # Extract the length for the first array
    try:
        token_id_length = next_int()
    except Exception as e:
        return None, None, e
    
    # Iterate over the first array based on the length
    for j in range(token_id_length):
        try:
            id_value = next_int()
            ids.append(hex(id_value))
        except Exception as e:
            return None, None, e
    
    # Extract the length for the second array
    try:
        length_value = next_int()
    except Exception as e:
        return None, None, e
    
    # Iterate over the second array based on the length
    for j in range(length_value):
        try:
            value = next_int()
            values.append(hex(value))
        except Exception as e:
            return None, None, e
    
    return ids, values, None