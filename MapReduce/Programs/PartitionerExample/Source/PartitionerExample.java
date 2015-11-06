import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PartitionerExample {
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
	//custom partitioner class to partition vowels and consonants
	//
	public static class TokenizerPartitioner 
	extends Partitioner<Text,IntWritable> {

		@Override
		public int getPartition(Text key, IntWritable value, int numReduceTasks) {
			String word = key.toString();

			if(numReduceTasks == 0) {
				return 0;
			} 
			if( word.substring(0,1).equals("a") || word.substring(0,1).equals("A") ){
				return 0; //pass to partition 0
			}
			else if( word.substring(0,1).equals("e") || word.substring(0,1).equals("E")  ){
				return 1 % numReduceTasks; //pass to partition 1
			}
			else if( word.substring(0,1).equals("i") || word.substring(0,1).equals("I") ){
				return 2 % numReduceTasks; //pass to partition 2
			}
			else if( word.substring(0,1).equals("o") || word.substring(0,1).equals("O") ){
				return 3 % numReduceTasks; //pass to partition 3
			}
			else if( word.substring(0,1).equals("u") || word.substring(0,1).equals("U") ){
				return 4 % numReduceTasks; //pass to partition 4
			} else {
				return 5 % numReduceTasks; //pass to partition 5 (consonants)
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
		Job job = Job.getInstance(conf, "MapReduce Partitioner Example");
		job.setNumReduceTasks(6); //will run with 6 reducers
		job.setJarByClass(PartitionerExample.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setPartitionerClass(TokenizerPartitioner.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}