#!/usr/bin/env python

import sys
import gzip
import json
from pprint import pprint
import bsddb3
from tqdm import tqdm

trans = {}
addresses = {}
fees = {}

old_txid = None
tx = {}

fp = gzip.open(sys.argv[1], 'rt')

def process_inputs(tx, utxo):
    fee = 0

    tx["error"] = False

    if "i" in tx:
        for index, txinput in enumerate(tx["i"]):    
            txkey = b"%s %u" % (txinput[0].encode(), txinput[1])
            txo = utxo.get(txkey, None)

            if txo is not None:
                txo = txo.split()
                tx["i"][index] = [txo[0].decode(), int(txo[1])]
                fee += int(txo[1])
                del utxo[txkey]
            else:
                tx["error"] = True

 
        for txo in tx["o"].values():
            fee -= txo[1]

    if fee < 0:
        tx["error"] = True

    tx["fee"] = fee

    return tx

def get_transaction(fp):
    old_txid = None
    tx = {}

    for line in fp:
        try:
            fields = line.strip().split()

            txid = fields[2]
            
            tx["timestamp"] = int(fields[1])

            addr = fields[4]
            value = int(fields[5])

            if txid != old_txid and old_txid is not None:
                tx["txid"] = old_txid 
                yield old_txid, tx
                tx = {}

            if fields[3] == "input":
                # coinbase
                if addr == "0000000000000000000000000000000000000000000000000000000000000000":
                    old_txid = txid
                    continue   

                if "i" not in tx:
                    tx["i"] = []

                tx["i"].append((addr, value))

            elif fields[3] == "output":
                if "o" not in tx:
                    tx["o"] = {}

                tx["o"][fields[6]] = (addr, value)

            else:
                print("WTF??", file=sys.stderr)

            old_txid = txid
        except Exception as e:
            pprint(e.__dict__)
            pass

old_timestamp = None

current = []

fp_out = gzip.open("transactions.out.json.gz", "wt")
fp_err = gzip.open("transactions.err.json.gz", "wt")

encoder = json.JSONEncoder()

trans_count = 0 
err_count = 0
output_count = 0

utxo = bsddb3.hashopen("utx.hashdb") #{}
trans = {}
for txid, tx in tqdm(get_transaction(fp), total=389431768):
    if "o" not in tx:
        output_count += 1
        continue

    #trans[txid] = tx.copy()

    for output, data in tx["o"].items():
        txkey = b"%s %u" % (txid.encode(), int(output))
        utxo[txkey] = b"%s %u" % (data[0].encode(), data[1])

    timestamp = tx["timestamp"]

    if timestamp != old_timestamp and old_timestamp is not None:
        for curr_tx in current:
            process_inputs(curr_tx, utxo)
            #trans[curr_tx["txid"]] = curr_tx

            trans_count += 1

            if curr_tx["error"] is False:
                del curr_tx["error"]
                print(encoder.encode(curr_tx), file=fp_out)
            else:
                del curr_tx["error"]
                err_count += 1
                print(encoder.encode(curr_tx), file=fp_err)

            if trans_count % 10000000 == 0:
                #print(trans_count, err_count, len(utxo), output_count, file=sys.stderr)
                utxo.sync()

        current = []

    current.append(tx)
    old_timestamp = timestamp
