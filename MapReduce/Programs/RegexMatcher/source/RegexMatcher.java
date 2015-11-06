import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RegexMatcher {
	//
	//map class definition
	//
	public static class SearchMapper
	extends Mapper<Object, Text, Text, IntWritable>{

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {

			Configuration conf = context.getConfiguration();
			String regex = conf.get("regex");
			String delimiter = conf.get("delimiter");

			StringTokenizer itr = new StringTokenizer(value.toString(), delimiter);
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());

				//if string matches regex, send to reducer this way 
				//we only count occurrences of matching strings
				if(word.toString().matches(regex))
					context.write(word, one);
			}
		}
	}
	//
	//reduce class definition
	//
	public static class IntSumReducer
	extends Reducer<Text,IntWritable,Text,IntWritable> {

		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context
				) throws IOException, InterruptedException {

			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			result.set(sum);
			context.write(key, result); 
		}
	}
	//
	//main method
	//
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		conf.set("regex",args[2]); //used to pass regex to mapper
		//set args[3] to space if empty
		if(args.length < 4) { 
			conf.set("delimiter"," "); 
		} else {
			conf.set("delimiter",args[3]); //used to pass delimiter to mapper
		}
		Job job = Job.getInstance(conf, "Regex Matcher");
		job.setJarByClass(RegexMatcher.class);
		job.setMapperClass(SearchMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}