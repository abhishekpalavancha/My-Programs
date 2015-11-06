# this mapper is written in Ruby
# as an example of Hadoop streaming
# It will read in a line of text as an
# argument and write each word to STDOUT
# with a value of 1

ARGF.each do |line|

   # truncate newline
   line = line.chomp

   # do nothing with lines shorter than a single character
   next if ! line || line.length < 1

   # split line on spaces to get individual words
   tokens = line.split(' ')

   value = 1

   # output to STDOUT
   # <key><tab><value><newline>
   tokens.each do |key|
   puts key + "\t" + value.to_s
     end

end