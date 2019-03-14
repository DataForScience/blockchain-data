#!/usr/bin/env python

import sys
import json
import gzip

with gzip.open(sys.argv[1], "rt") as fp:
    for tx in fp:
        tx = json.loads(tx)

        if tx["fee"] < 0:
            continue

        total = 0
        tx_out = tx["o"].values()

        for txo in tx_out:
            total += txo[1]

        if total == 0:
            print("woot")

        if "i" in tx:
            tx_in = tx["i"]

            for txin in tx_in:
                for txo in tx_out:
                    if total > 0:
                        print(tx["timestamp"], txin[0], txo[0], txin[1]*txo[1]/total, tx["fee"], tx["txid"])
                    else:
                        print(tx["timestamp"], txin[0], txo[0], 0, tx["fee"], tx["txid"])

        else:
            for txo in tx_out:
                print(tx["timestamp"], "coinbase", txo[0], txo[1], tx["fee"], tx["txid"])

