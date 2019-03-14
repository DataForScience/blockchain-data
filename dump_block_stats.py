#!/usr/bin/env python

import os 
from blockchain_parser.blockchain import Blockchain
from pprint import pprint
import gzip
import calendar
import sys

directory = sys.argv[1]

output_file = os.path.join(".", "blocks.dat.gz")
temp_file = os.path.join(".", "blocks.dat.gz_tmp")
error_file = os.path.join(".", "error.dat")

if os.path.exists(output_file):
    sys.exit(2)

fp_out = gzip.open(temp_file, "wt")
fp_err = open(error_file, "at")

blockchain = Blockchain(directory)

txid = 0
line_count = 0
block_count = 0

print("block_count timestamp hash previous_block_hash n_transactions txid reward difficulty bits nonce size", file=fp_out)

for block in blockchain.get_unordered_blocks():
    header = block.header
    timestamp = calendar.timegm(header.timestamp.timetuple())

    print(block_count, timestamp, block.hash, header.previous_block_hash, 
         block.n_transactions, block.transactions[0].txid, 
         block.transactions[0].outputs[0].value, header.difficulty, 
         header.bits, header.nonce, block.size, file=fp_out)

    block_count += 1

    if block_count % 10000 == 0:
        print(block_count, file=sys.stderr)

fp_out.close()
fp_err.close()

os.rename(temp_file, output_file)