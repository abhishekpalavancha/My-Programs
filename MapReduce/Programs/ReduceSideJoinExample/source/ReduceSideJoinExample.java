import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ReduceSideJoinExample {
	//
	//map class definition
	//
	public static class SearchMapper
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

			URI[] addressOfFileToJoin = context.getCacheFiles();
			Path fileToJoin = new Path(addressOfFileToJoin[0]);
			FileSystem fs = FileSystem.get(new Configuration());
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(fileToJoin)));

			//array to hold the words to be excluded from results
			String line = null;
			List<String> wordsToExclude = new ArrayList<String>();

			while((line = br.readLine()) != null) {
				wordsToExclude.add(line);
			}

			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			result.set(sum);

			//loop through the words read in from file
			//to ensure that none match the key
			//if no match then write to file
			boolean write = true;
			for(String word : wordsToExclude) {
				if(word.equalsIgnoreCase(key.toString())) {
					write = false;//do not write word to output
				} 
			}
			if(write == true)
				context.write(key, result);
		}
	}
	//
	//main method
	//
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "MapReduce Reduce Side Join Example");
		URI fileAddress = new URI(args[2]);
		job.addCacheFile(fileAddress);
		job.setJarByClass(ReduceSideJoinExample.class);
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