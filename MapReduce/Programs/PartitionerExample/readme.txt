This MapReduce program demonstrates the use of a partitioner. The partitioner in this example sends
key value pairs to a specific reducer based on whether or not the key begins with a vowel.
Keys beginning with A, E, I, O, and U, are sent to reducers 0, 1, 2, 3, and 4, respectively.
All remaining key/value pairs (including those which begin with consonants and symbols) are sent
to reducer 5.