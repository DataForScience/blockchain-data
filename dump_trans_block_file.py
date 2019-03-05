#!/usr/bin/env python

import os 
from blockchain_parser.blockchain import Blockchain, Blockchain_file
from pprint import pprint
import gzip
import calendar
import sys

filename = sys.argv[1]

basename = os.path.basename(filename)
temp_file = os.path.join("transactions", basename + ".gz_tmp")
output_file = os.path.join("transactions", basename + ".gz")
error_file = os.path.join("transactions", "error.dat")

if os.path.exists(output_file):
    sys.exit(2)

fp_out = gzip.open(temp_file, "wt")
fp_err = open(error_file, "at")

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
            print(output_file, file=fp_err)
            pprint(e.__dict__, stream=fp_err)
            print(txid, file=fp_err)


fp_out.close()
fp_err.close()

os.rename(temp_file, output_file)