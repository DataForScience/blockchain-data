import os 
from blockchain_parser.blockchain import Blockchain, Blockchain_file
from pprint import pprint
import calendar
import sys

data_dir = "/Users/bgoncalves/Downloads/Movies/blockchain/blocks/"

blockchain = Blockchain(data_dir)

txid = 0
line_count = 0

for block in blockchain.get_unordered_blocks():
    timestamp = calendar.timegm(block.header.timestamp.timetuple())

    for tx in block.transactions:

        txid += 1

        if txid % 1000 == 0:
            print(txid, line_count, timestamp, file=sys.stderr)

        for inp in tx.inputs:
            trans = inp.transaction_hash
            out_id = inp.transaction_index

            print(txid, timestamp, tx.hash, "input", trans, out_id)
            line_count += 1

        for no, output in enumerate(tx.outputs):
            for address in output.addresses:
                print(txid, timestamp, tx.hash, "output", address.address, output.value)
                line_count += 1