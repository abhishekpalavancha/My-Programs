This program demonstrates how MapReduce can be used 
to analyze server logs. The server log used for this 
demonstration was generated in a Ruby program which I wrote,
that uses the Faker gem to generate fake data. In this case,
an IPV4 address and a time-stamp has been generated.

The mapper, partitioner and reducer all accept a NullWritable 
as a value. This is done to purposefully prevent aggregation 
in the reduce phase, since we only want to divide the
records of our input file into smaller, more manageable files,
for each type of IP address prefix.

The types of IP address prefixes which will be partitioned
are:

10.x.x.x, 192.x.x.x, 174.x.x.x, meaning the logged access attempt was from
an internal (intranet) machine
41.x.x.x, 102.x.x.x, 105.x.x.x, meaning the IP address is an AfriNIC allocation
17.x.x.x, meaning it was an access request from a machine inside of Apple's network,
9.x.x.x, meaning it was an access request from a machine 
inside of IBM's network 

The Ruby script which I used to generate a fake server log for 
testing is also included in the source folder. 
