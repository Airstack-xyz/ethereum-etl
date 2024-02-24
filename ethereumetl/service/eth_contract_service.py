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
from eth_utils import function_signature_to_4byte_selector

from ethereum_dasm.evmdasm import EvmCode, Contract


class EthContractService:

    def get_function_sighashes(self, bytecode):
        bytecode = clean_bytecode(bytecode)
        if bytecode is not None:
            evm_code = EvmCode(contract=Contract(bytecode=bytecode), static_analysis=False, dynamic_analysis=False)
            evm_code.disassemble(bytecode)
            tmp = list()
            basic_blocks = evm_code.basicblocks
            if basic_blocks and len(basic_blocks) > 0:
                for i in range(len(basic_blocks)):
                    init_block = basic_blocks[i]
                    instructions = init_block.instructions
                    push4_instructions = [inst for inst in instructions if inst.name == 'PUSH4']
                    tmp = tmp + list(set('0x' + inst.operand for inst in push4_instructions))
                return sorted(list(set(tmp)))
            else:
                return []
        else:
            return []
        

    # https://github.com/ethereum/EIPs/blob/master/EIPS/eip-20.md
    # https://github.com/OpenZeppelin/openzeppelin-solidity/blob/master/contracts/token/ERC20/ERC20.sol
    def is_erc20_contract(self, function_sighashes):
        c = ContractWrapper(function_sighashes)
        return c.implements('totalSupply()') and \
               c.implements('balanceOf(address)') and \
               c.implements('transfer(address,uint256)') and \
               c.implements('transferFrom(address,address,uint256)') and \
               c.implements('approve(address,uint256)') and \
               c.implements('allowance(address,address)')
    
    def is_erc20_contract_v1(self, bytecode):
          if bytecode is None or bytecode == '0x':
              return False
          return (
            get_function_sighash_without0x('balanceOf(address)') in bytecode and
            get_function_sighash_without0x('totalSupply()') in bytecode and 
            get_function_sighash_without0x('allowance(address,address)') in bytecode and
            get_function_sighash_without0x('approve(address,uint256)') in bytecode and
            get_function_sighash_without0x('transfer(address,uint256)') in bytecode and
            get_function_sighash_without0x('transferFrom(address,address,uint256)') in bytecode
        )
            

    # https://github.com/ethereum/EIPs/blob/master/EIPS/eip-721.md
    # https://github.com/OpenZeppelin/openzeppelin-solidity/blob/master/contracts/token/ERC721/ERC721Basic.sol
    # Doesn't check the below ERC721 methods to match CryptoKitties contract
    # getApproved(uint256)
    # setApprovalForAll(address,bool)
    # isApprovedForAll(address,address)
    # transferFrom(address,address,uint256)
    # safeTransferFrom(address,address,uint256)
    # safeTransferFrom(address,address,uint256,bytes)
    def is_erc721_contract(self, function_sighashes):
        c = ContractWrapper(function_sighashes)
        return c.implements('balanceOf(address)') and \
               c.implements('ownerOf(uint256)') and \
               c.implements_any_of('transfer(address,uint256)', 'transferFrom(address,address,uint256)') and \
               c.implements('approve(address,uint256)')
    
    def is_erc721_contract_v1(self, bytecode):
        if bytecode is None or bytecode == '0x':
              return False
        return (
            get_function_sighash_without0x('balanceOf(address)') in bytecode and
            get_function_sighash_without0x('ownerOf(uint256)') in bytecode and 
            get_function_sighash_without0x('approve(address,uint256)') in bytecode and
            (get_function_sighash_without0x('transfer(address,uint256)') in bytecode or
            get_function_sighash_without0x('transferFrom(address,address,uint256)') in bytecode)
        )

    
    def is_erc1155_contract(self, function_sighashes):
        c = ContractWrapper(function_sighashes)
        return c.implements('transferSingle(address,address,address,uint256,uint256)') and \
               c.implements('transferBatch(address,address,address,uint256[],uint256[])')
    
    def is_erc1155_contract_v1(self, bytecode):
        if bytecode is None or bytecode == '0x':
              return False
        return (
            get_function_sighash_without0x('TransferSingle(address,address,address,uint256,uint256)') in bytecode and
            get_function_sighash_without0x('TransferBatch(address,address,address,uint256[],uint256[])') in bytecode
        )
    
    #'0x3659cfe6'  -- upgradeTo(address)
    #'0x4f1ef286' -- upgradeToAndCall(address,bytes)
    #'0x5c60da1b'  -- implementation()
    #'0x8f283970'  changeAdmin(address)
    #'0xf851a440'  -- admin()  

    def is_proxy_contract(self, function_sighashes):
        c = ContractWrapper(function_sighashes)
        return c.implements('implementation()')
    
    def is_proxy_contract_v1(self, bytecode):
         if bytecode is None or bytecode == '0x':
              return False
         return (
            get_function_sighash_without0x('implementation()') in bytecode 
        )
              

def clean_bytecode(bytecode):
    if bytecode is None or bytecode == '0x':
        return None
    elif bytecode.startswith('0x'):
        return bytecode[2:]
    else:
        return bytecode


def get_function_sighash(signature):
    return '0x' + function_signature_to_4byte_selector(signature).hex()

def get_function_sighash_without0x(signature):
    return function_signature_to_4byte_selector(signature).hex()


class ContractWrapper:
    def __init__(self, sighashes):
        self.sighashes = sighashes

    def implements(self, function_signature):
        sighash = get_function_sighash(function_signature)
        return sighash in self.sighashes

    def implements_any_of(self, *function_signatures):
        return any(self.implements(function_signature) for function_signature in function_signatures)
