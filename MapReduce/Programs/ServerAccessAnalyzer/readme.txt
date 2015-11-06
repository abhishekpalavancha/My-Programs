This program demonstrates how MapReduce can be used 
to analyze server logs. The server log used for this 
demonstration was generated in a Ruby program which I wrote,
that uses the Faker gem to generate fake data. In this case,
an IPV4 address and a time-stamp has been generated.

The mapper, partitioner and reducer all accept a NullWritable 
as a value. This is done to purposefully prevent aggregation 
in the reduce phase, since we only want to divide the
records of our input file into smaller, more manageable files,
for each month of 2014.

The Ruby script which I used to generate a fake server log for 
testing is also included in the source folder. 
