The RegexMatcher program is a MapReduce program which accepts two 
argument at run time, the first is the regular expression to be
matched on, and the second is the delimiter on which to split tokens.
If no delimiter is provided, the program defaults to a delimiter of space.

I originally ran this program using the following command:
hadoop jar RegexMatcher.jar RegexMatcher The_Island_of_Doctor_Moreau.txt RegexMatcher .+octor

The regex ".+octor" matches any token with the string "octor", proceeded by at 
least one character, any number of times.  