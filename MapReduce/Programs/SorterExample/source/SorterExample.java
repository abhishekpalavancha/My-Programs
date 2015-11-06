import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SorterExample {
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
	//sorter class definition
	//
	public static class DescendingKeyComparator 
	extends WritableComparator {
		protected DescendingKeyComparator() {
			super(Text.class, true);
		}
		//by comparing the total bytes of our keys, we can 
		//determine which value represents a higher number,
		//and can rank those highest in the output
		public int compare(IntWritable w1, IntWritable w2) {
			return -1 * w1.compareTo(w2);
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
		Job job = Job.getInstance(conf, "MapReduce Sorter Example");
		job.setJarByClass(SorterExample.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setSortComparatorClass(DescendingKeyComparator.class); //set comparator class
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}