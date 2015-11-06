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

public class CounterExample {
	//
	//enum with each value representing a counter
	//
	static enum Vowels { BEGINS_WITH_A, BEGINS_WITH_E, BEGINS_WITH_I, BEGINS_WITH_O, BEGINS_WITH_U, ALL }
	//
	//map class definition
	//
	public static class TokenizerMapper
	extends Mapper<Object, Text, Text, IntWritable>{

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
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
			//
			//counters tracking the usage of vowels A,E,I,O,U
			//
			int sum = 0;
			String token = key.toString();
			if( token.substring(0,1).equals("a") || token.substring(0,1).equals("A") ){
				context.getCounter(Vowels.BEGINS_WITH_A).increment(1);
			}
			else if( token.substring(0,1).equals("e") || token.substring(0,1).equals("E")  ){
				context.getCounter(Vowels.BEGINS_WITH_E).increment(1);
			}
			else if( token.substring(0,1).equals("i") || token.substring(0,1).equals("I") ){
				context.getCounter(Vowels.BEGINS_WITH_I).increment(1);
			}
			else if( token.substring(0,1).equals("o") || token.substring(0,1).equals("O") ){
				context.getCounter(Vowels.BEGINS_WITH_O).increment(1);
			}
			else if( token.substring(0,1).equals("u") || token.substring(0,1).equals("U") ){
				context.getCounter(Vowels.BEGINS_WITH_U).increment(1);
			}
			context.getCounter(Vowels.ALL).increment(1);
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
		Job job = Job.getInstance(conf, "MapReduce Counter Example");
		job.setJarByClass(CounterExample.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}