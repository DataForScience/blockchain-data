#!/usr/bin/env python

import gzip
import sys
import os

from os import listdir
from os.path import isfile, join

from heapq import *

files = []

onlyfiles = sys.argv[2:]

#onlyfiles = [join(sys.argv[1], f) for f in listdir(sys.argv[1]) if isfile(join(sys.argv[1], f)) and f.startswith("blk0")]
fp_out = gzip.open(os.path.join("sorted", "data_sorted." + sys.argv[1] + ".dat.gz"), "wt")

for i in range(len(onlyfiles)):
    fp = gzip.open(onlyfiles[i], "rt")
    line = fp.readline()
    time = int(line.strip().split()[1])

    files.append((time, line, fp))

print("Found", len(files), "block files...", file=sys.stderr)

heapify(files)

line_count = 0

while len(files)>0:
    old_item = files[0] # get the oldest line
    line = old_item[1]

    line_count += 1

    if line_count % 1000000 == 0:
        print(line_count, file=sys.stderr)

    print(line.strip(), file=fp_out)
    fp = old_item[2]
    line = fp.readline()

    if len(line) == 0:
        old_item = heappop(files)
        fp.close()
        continue

    time = int(line.strip().split()[1])

    new_item = (time, line, fp)
    heapreplace(files, new_item)

fp_out.close()