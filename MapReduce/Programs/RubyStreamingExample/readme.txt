This MapReduce program is an example of how Hadoop streaming
can be used to create MapReduce programs in Ruby.

This script is a basic word count program.
The mapper will read input from ARGF one line of
text at a time, and then split that line on spaces.

These words will be written to STDOUT, and will eventually be
raed in by the reducer via ARGF. They values will be aggregated for each
key, and will be written to STDOUT again.

