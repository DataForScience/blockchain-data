#!/usr/bin/env python

import os
import struct
from binascii import hexlify


data_dir = "/Users/bgoncalves/Downloads/Movies/blockchain/blocks/"
block_file = "blk00000.dat"
#block_file = "blk01500.dat"

filename = os.path.join(data_dir, block_file)

def read_char(fp, size=32):
    fmt = "<%02us" % size
    return struct.unpack(fmt, fp.read(size))[0]

def read_varint(fp):
    size = struct.unpack("<B", fp.read(1))[0]

    if size < 253:
        return size

    if size == 253:
        return read_uint(fp, 2)
    elif size == 254:
        return reat_uint(fp, 3)
    elif size == 255:
        return read_uint(fp, 4)

def read_uint(fp, size=4):
    
    if size == 2:
        fmt = "<H"
    elif size == 4:
        fmt = "<I"
    elif size == 8:
        fmt = "<Q"

    return struct.unpack(fmt, fp.read(size))[0]

def read_int(fp, size=4):
    
    if size == 2:
        fmt = "<h"
    elif size == 4:
        fmt = "<i"
    elif size == 8:
        fmt = "<q"

    return struct.unpack(fmt, fp.read(size))[0]

def hashStr(data):
    return str(hexlify(data[::-1]).decode("utf-8"))

with open(filename, "rb") as fp:
    while True:
        block_data = {}
        block_data["magic"] = hashStr(read_char(fp, 4))
        block_data["size"] = read_int(fp, 4)

        header = {}
        header["version"] = read_uint(fp, 4)
        header["prev-hash"] = hashStr(read_char(fp, 32))
        header["merkle"] = hashStr(fp.read(32))
        header["time"] = read_int(fp)
        header["difficulty"] = read_uint(fp, 4)
        header["nounce"] = read_uint(fp, 4)

        block_data["header"] = header
        block_data["tsize"] = read_varint(fp)

        if  block_data["tsize"] > 10:
            print("Woot")

        block_data["transactions"] = []

        for tsize in range(block_data["tsize"]):
            trans = {}

            trans["tver"] = int.from_bytes(fp.read(4), 'little')
            trans["inputs"] = []
            trans["outputs"] = []

            if tsize == "W": # coinbase
                trans["hash"] = hashStr(read_char(fp, 32))
                trans["index"] = read_uint(fp, 4)
                trans["script-bytes"] = read_varint(fp)
                trans["height"] = read_char(fp, trans["script-bytes"])
                trans["sequence"] = read_uint(fp, 4)
            else:
                trans["input_count"] = read_varint(fp)

                for __ in range(trans["input_count"]):
                    tx_input = {}
                    tx_input["prev-hash"] = hashStr(fp.read(32))
                    tx_input["vout"] = int.from_bytes(fp.read(4), 'little')
                    tx_input["sigsize"] = read_varint(fp)
                    tx_input["ScriptSig"] = fp.read(tx_input["sigsize"])
                    tx_input["seqNo"] = int.from_bytes(fp.read(4), 'little')

                    trans["inputs"].append(tx_input)

            trans["output_count"] = read_varint(fp)

            for __ in range(trans["output_count"]):
                tx_output = {}
                tx_output["value"] = int.from_bytes(fp.read(8), 'little')
                tx_output["scriptpubkey"] = read_varint(fp)
                tx_output["publickey"] = hashStr(fp.read(tx_output["scriptpubkey"]))
                
                trans["outputs"].append(tx_output)

            trans["locktime"] = int.from_bytes(fp.read(4), 'little')

            block_data["transactions"].append(trans)

