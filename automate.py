import os

# inputs = [
# 'input_50000.txt'
# ,'input_500000.txt'
# ,'input_5000000.txt'
# ,'input_50000000.txt'
# ,'input_20000000.txt'
# ,'input_10000000.txt'
# ]

inputs = [ 25, 50, 100, 250, 500]

# for inp in inputs:
#     print(inp)
#     os.system("python 20171148.py "+inp+" output.txt 50 asc c2")

for inp in inputs:
    print(inp)
    os.system("python msort.py input500.txt output.txt "+ str(inp) +" asc c2")