import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class IpPrefixAnalyzer {
	//this mapper will treat one row from the input file
	//as the key, with a value of null. This allows the 
	//reducer to write all input rows as output
	public static class TokenizerMapper
	extends Mapper<Object, Text, Text, NullWritable>{

		private Text row = new Text();

		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {

			StringTokenizer itr = new StringTokenizer(value.toString(),"\n");
			while (itr.hasMoreTokens()) {
				row.set(itr.nextToken());
				context.write(row, NullWritable.get());
			}
		}
	}
	//partition on IP prefix, to distribute our access log
	//into single lists for each type of prefix
	//
	public static class MonthPartitioner 
	extends Partitioner<Text,NullWritable> {

		@Override
		public int getPartition(Text key, NullWritable value, int numReduceTasks) {
			String[] row = key.toString().split("\u0004");
			String[] ipAddress = row[0].split("\\.");
			String ipPrefix = ipAddress[0]; 

			if(numReduceTasks == 0) {
				return 0;
			} 
			//Private networks, e.g. intranets
			if( ipPrefix.equals("192") || ipPrefix.equals("172") || ipPrefix.equals("10") ){
				return 0; //pass to partition 0
			}
			//AfriNIC allocations for IP addresses in Africa
			else if( ipPrefix.equals("41") || ipPrefix.equals("102") || ipPrefix.equals("105")){
				return 1 % numReduceTasks; //pass to partition 1
			}
			//European allocations for IP addresses
			else if( ipPrefix.equals("81") || ipPrefix.equals("217") || ipPrefix.equals("62") ){
				return 2 % numReduceTasks; //pass to partition 2
			}
			//Latin America and the Caribbean
			else if( ipPrefix.equals("200") ){
				return 3 % numReduceTasks; //pass to partition 3
			}
			//IBM
			else if( ipPrefix.equals("9") ){
				return 4 % numReduceTasks; //pass to partition 4
			}
			//Apple
			else if( ipPrefix.equals("17") ){
				return 5 % numReduceTasks; //pass to partition 5
			} else {
				return 6 % numReduceTasks; //pass to partition 6 (default case)
			}
		}
	}  
	//reduce class definition will not aggregate and will
	//simply group records by IP prefix
	//
	public static class IntSumReducer
	extends Reducer<Text,NullWritable,Text,NullWritable> {

		public void reduce(Text key, Iterable<NullWritable> values,
				Context context
				) throws IOException, InterruptedException {


			context.write(key, NullWritable.get());
		}
	}
	//
	//main method
	//
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "User Access Analyzer");
		job.setNumReduceTasks(7); //will run with 7 reducers
		job.setJarByClass(MonthPartitioner.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setPartitionerClass(MonthPartitioner.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}