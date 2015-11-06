This MapReduce program demonstrates a map-side search. The user supplies a 
command line argument of a word to be searched, and only keys which match 
that value in the mapper are passed to the reducer for aggregation. 
This results in only instances of that specific word being counted.