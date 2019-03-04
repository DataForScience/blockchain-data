#!/usr/bin/env python

import os 
from blockchain_parser.blockchain import Blockchain, Blockchain_file
from pprint import pprint
import gzip
import calendar
import sys

filename = sys.argv[1]

basename = os.path.basename(filename)
output_file = os.path.join("transactions", basename+ ".gz")

if os.path.exists(output_file):
    sys.exit(2)

fp_out = gzip.open(output_file, "wt")

blockchain = Blockchain_file(filename)

txid = 0
line_count = 0

for block in blockchain.get_unordered_blocks():
    timestamp = calendar.timegm(block.header.timestamp.timetuple())

    for tx in block.transactions:
        try:
            txid += 1

            if txid % 100000 == 0:
                print(txid, line_count, timestamp, file=sys.stderr)

            for inp in tx.inputs:
                trans = inp.transaction_hash
                out_id = inp.transaction_index

                print(txid, timestamp, tx.hash, "input", trans, out_id, file=fp_out)
                line_count += 1

            for no, output in enumerate(tx.outputs):
                for address in output.addresses:
                    print(txid, timestamp, tx.hash, "output", address.address, output.value, file=fp_out)
                    line_count += 1
        except Exception as e:
            pprint(e.__dict__, file=sys.stderr)

fp_out.close()