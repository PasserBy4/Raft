#!/usr/bin/env python

import os, time, sys
from os.path import isfile, join
import shutil

os.system('./stopall >/dev/null 2>/dev/null')
os.system('./build >/dev/null 2>/dev/null')

test_output = 'test_output'
tests = 'tests'
if len(sys.argv) == 2:
    tests = sys.argv[1]
try:
    # shutil.rmtree(test_output)
    print("yeah")
except:
    pass
# os.mkdir(test_output)
for f in os.listdir(tests):
    abs_f = join(tests, f)
    if isfile(abs_f):
        if f[len(f) - len('.input'):] == '.input':
            fn = f[:len(f) - len('.input')]
            print(fn),
            os.system('python proxy.py < ' + abs_f + \
                    ' 2> ' + join(test_output, fn+'.err') +\
                    ' > ' + join(test_output, fn+'.output'))

            os.system('./stopall >/dev/null 2>/dev/null')

            with open(join(test_output, fn+'.output')) as fi:
                    out = fi.read().strip().split('\n')
            with open(join(tests, fn+'.output')) as fi:
                    std = fi.read().strip().split('\n')
            result = True
            print(out)
            print(std)
            out_index = 0
            for s in std:
                json = eval(s)
                prev = None
                for i in range(json['count']):
                    # check the number of lines in output
                    if out_index >= len(out):
                        result = False
                        print("1")
                        break
                    # check if all the outputs are identical
                    if prev is not None and prev != out[out_index]:
                        result = False
                        print("2")
                    prev = out[out_index]
                    print("prev", prev)
                    # check if the output satisfies the requirement
                    out_data = set(out[out_index].split(','))
                    print("out_data", out_data)
                    std_mandatory = set(json['mandatory'].split(','))
                    print("std_mandatory", std_mandatory)
                    std_all = set(json['optional'].split(',')).union(std_mandatory)
                    print("std_all", std_all)
                    if not out_data.issubset(std_all):
                        result = False
                        print("3")
                    if not out_data.issuperset(std_mandatory):
                        result = False
                        print("4")

                    out_index += 1

            if result:
                print('correct')
            else:
                print('wrong')
            time.sleep(2)
            break