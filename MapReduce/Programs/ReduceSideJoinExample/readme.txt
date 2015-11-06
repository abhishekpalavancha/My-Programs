This MapReduce program demonstrates a reducer side join to a file. The file which is joined
is titled 'ExcludedWords.txt' and exists on the Hadoop Distributed File System (HDFS). The
address of the file on HDFS is passed to the MapReduce program at run time via a command line
argument. 

The reducer will exclude any key from the final output if that key matches any of the words
listed in the 'ExcludedWords.txt' file. This MapReduce program behaves similarly to a SQL 
query which would filter out unwanted results in the where clause by using the 'NOT IN(...)' 
keyword.