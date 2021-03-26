#!/usr/bin/python3

import sys

inside_code = False
with open(sys.argv[1], 'r') as f:
    for line in f:
        toggle_code = line[:3]=='```'
        if inside_code and not toggle_code:
            print(line, end='')
        if inside_code and toggle_code:
            print('\n')
        if toggle_code:
            inside_code = not inside_code
