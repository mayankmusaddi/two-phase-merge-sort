import sys
import os
import math
import heapq
import threading
import numpy as np
import time

class Tuple(object):
    def __init__(self, val, columns, order, fp = -1):
        self.val = val
        self.fp = fp
        self.columns = columns
        self.order = order
    
    def __lt__(self, other):
        for column in self.columns:
            if self.val[column] != other.val[column]:
                res = (self.val[column] < other.val[column])
                return res if not self.order else not res
        return False

def get_tuple(line, sizes):
    tup = []
    start = 0
    for size in sizes:
        tup.append(line[start:start+size])
        start += size+2
    return tup

def run_phase2(FILENAME, num_splits, num_mem_tuples, columns, sizes, order):
    print("##running phase-2")
    fp = []
    for i in range(0,num_splits):
        f = open(f'temp{i}.txt', 'r')
        fp.append(f)

    print("Sorting...")
    h = []
    for i in range(0,num_splits):
        line = fp[i].readline()
        tup = get_tuple(line, sizes)
        line = Tuple(tup, columns, order, i)
        heapq.heappush(h, line)

    print("Writing to disk")
    w = open(FILENAME, 'w')
    while len(h) > 0:
        s = heapq.heappop(h)
        w.write('  '.join(s.val)+"\n")
        i = s.fp
        line = fp[i].readline()
        if line != '':
            tup = get_tuple(line, sizes)
            line = Tuple(tup, columns, order, i)
            heapq.heappush(h, line)
        else:
            os.remove(f'temp{i}.txt')
    w.close()

    for i in range(0,num_splits):
        fp[i].close()

def sort_sublist(lines, columns, order, j):
    print("sorting #{} sublist".format(j+1))
    lines.sort()

    with open(f'temp{j}.txt', 'w') as w:
        for line in lines:
            w.write('  '.join(line.val)+"\n")

    print("Writing to disk #{}".format(j+1))

def run_phase1(FILENAME, num_mem_tuples, columns, sizes, order):
    lines = []
    fn = 0
    with open(FILENAME, "r") as f:
        for i,line in enumerate(f):
            tup = get_tuple(line, sizes)
            lines.append(Tuple(tup, columns, order))
            if (i+1) % num_mem_tuples == 0:
                sort_sublist(lines, columns, order, fn)
                fn += 1
                lines = []
        if len(lines) != 0:
            sort_sublist(lines, columns, order, fn)

def run_phase1_parallel(FILENAME, num_threads, num_mem_tuples, columns, sizes, order):
    lines = []
    fn = 0
    with open(FILENAME, "r") as f:
        for i,line in enumerate(f): 
            tup = get_tuple(line, sizes)
            lines.append(Tuple(tup, columns, order))
            if (i+1) % num_mem_tuples == 0:
                thread_lines = np.array_split(lines, num_threads)
                threads = []
                for j in range(0, num_threads):
                    lines = thread_lines[j]
                    thread = threading.Thread(target=sort_sublist, args=(lines, columns, order, fn))
                    threads.append(thread)
                    thread.start()
                    fn += 1
                for thread in threads:
                    thread.join()
                lines = []
        if len(lines) != 0:
            thread_lines = np.array_split(lines, num_threads)
            threads = []
            for j in range(0, num_threads):
                lines = thread_lines[j]
                thread = threading.Thread(target=sort_sublist, args=(lines, columns, order, fn))
                threads.append(thread)
                thread.start()
                fn += 1
            for thread in threads:
                thread.join()

def get_total_tuples(FILENAME):
    if not os.path.isfile(FILENAME):
        print("Input File Doesn't Exist")
        exit(0)
        
    count = 0
    with open(FILENAME, 'r') as f:
        for _ in f:
            count += 1
    return count

def read_metadata(FILENAME = "metadata.txt"):
    columns = []
    sizes = []
    with open(FILENAME, 'r') as f:
        lines = f.readlines()
        for line in lines:
            t = line.split(",")
            t = [x.strip() for x in t]
            columns.append(t[0])
            sizes.append(int(t[1]))
    return columns, sizes

def read_args():
    n_args = len(sys.argv) - 1
    if n_args < 5:
        print("Invalid Arguments")
        exit(0)

    i = 1
    input_file = sys.argv[i]
    i += 1
    output_file = sys.argv[i]
    i += 1
    mem_size = sys.argv[i]
    i += 1
    if mem_size.isdigit():
        mem_size = int(mem_size)
    else:
        print("Invalid Mem Size Argument")
        exit(0)

    num_threads = 0
    tok = sys.argv[i]
    if tok.isdigit():
        num_threads = int(tok)
        i += 1
    
    order = sys.argv[i].lower()
    i += 1
    if order != 'asc' and order != 'desc':
        print("Invalid Ordering Argument")
        exit(0)
    else:
        order = True if order == 'desc' else False

    if i > n_args:
        print("Invalid Arguments")
        exit(0)
    
    columns = []
    while i <= n_args:
        column = sys.argv[i]
        columns.append(column)
        i += 1

    return input_file, output_file, mem_size, num_threads, order, columns

def get_col_ind(total_columns, column_args):
    if len(set(column_args)) != len(column_args):
        print("Duplicated Columns Arguments")
        exit(0)
    try:
        columns = [total_columns.index(i) for i in column_args]
    except:
        print("Column in Argument Not Present")
        exit(0)
    return columns

if __name__ == '__main__':
    start_time = time.time()

    total_columns, sizes = read_metadata()
    tuple_size = sum(sizes)
    input_file, output_file, mem_size, num_threads, order, column_args = read_args()
    columns = get_col_ind(total_columns, column_args)
    
    total_tuples = get_total_tuples(input_file)
    num_mem_tuples = math.floor(mem_size*1000000 / tuple_size)
    num_splits = math.ceil(total_tuples / num_mem_tuples)

    # print(input_file, output_file, mem_size, num_threads, order, column_args)
    # print(num_mem_tuples, num_splits)
    # print(total_tuples)

    if num_threads > 0:
        num_splits = num_splits * num_threads

    if num_splits > num_mem_tuples:
        print("Infeasible condition for two phase merge")
        exit(0)

    print("####start execution")
    print("####running Phase-1")
    print("Number of sub-files (splits): ", num_splits)

    if num_threads > 0:
        run_phase1_parallel(input_file, num_threads, num_mem_tuples, columns, sizes, order)
    else:
        run_phase1(input_file, num_mem_tuples, columns, sizes, order)

    p1_end_time = time.time()
    print("Phase1 Time Taken : ", p1_end_time - start_time)

    run_phase2(output_file, num_splits, num_mem_tuples, columns, sizes, order)
    end_time = time.time()

    # print("###completed execution")
    print("Time Taken : ", end_time - start_time)
