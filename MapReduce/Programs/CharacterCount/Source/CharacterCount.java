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

public class CharacterCount {
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
			itr = new StringTokenizer(value.toString());


			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				//
				//this loop will cause the mapper to treate each character
				//of each word as a key, effectively causing the entire
				//program to count characters instead of words
				//
				for(int i = 0; i < word.toString().length(); i++) {
					String singleChar = word.toString().substring(i,i+1);
					Text charAsText = new Text(singleChar);
					context.write(charAsText, one);
				}
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
		Job job = Job.getInstance(conf, "MapReduce Character Count");
		job.setJarByClass(CharacterCount.class);
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