#!/usr/bin/env python

import gzip
import sys
import os

from heapq import *

files = []

fp_out = gzip.open("data_sorted.gz", "wt")

for i in range(1, len(sys.argv)):
    fp = gzip.open(sys.argv[i], "rt")
    line = fp.readline()
    time = int(line.strip().split()[1])

    files.append((time, line, fp))

heapify(files)

line_count = 0

while len(files)>0:
    old_item = files[0] # get the oldest line
    line = old_item[1]

    line_count += 1

    if line_count % 10000 == 0:
        print(line_count, file=sys.stderr)

    print(line.strip(), file=fp_out)
    fp = old_item[2]
    line = fp.readline()

    if len(line) == 0:
        old_item = heappop(files)
        continue

    time = int(line.strip().split()[1])

    new_item = (time, line, fp)
    heapreplace(files, new_item)

fp_out.close()