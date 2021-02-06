# Two Phase Merge Sort

## Introduction
This is an implementation of the two phase merge sort algorithm which is required for sorting of high number of records in a small memory space. This is done in two phase by sorting sublists and writing them to the disks. Finally merging the sorted sublists present in the disk while making sure that the memory contraints are maintained.

## Execution Instructions
Download the requirements using the command:
```
pip3 intall -r requirements.txt
```

Then execute the `msort.py` file containing the main code. The arguments of which are as follows: <br>
1. Input file name (containing the raw records) <br>
2. Output filename (containing sorted records) <br> 
3. Main memory size (in MB) <br>
4. Number of threads (optional) <br>
5. Order code (asc / desc) asc : ascending, desc : descending. <br>
6. ColumnNames c1 c2 <br>

An example execution command is as follows:
```
python3 msort.py input.txt output.txt 30 5 desc c2 c1
```
Here the column names and their corresponding sizes should be present in the `metadata.txt` file, in the same format as initially present from the example `input.txt` file in this repository.