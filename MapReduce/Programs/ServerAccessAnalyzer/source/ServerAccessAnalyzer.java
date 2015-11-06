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

public class ServerAccessAnalyzer {
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
	//partition on access date, to distribute our access log
	//containing an entire years worth of data into a 
	//single list for each month
	public static class MonthPartitioner 
	extends Partitioner<Text,NullWritable> {

		@Override
		public int getPartition(Text key, NullWritable value, int numReduceTasks) {
			String[] row = key.toString().split("\u0004");
			String accessDate = row[1];

			if(numReduceTasks == 0) {
				return 0;
			} 
			if( accessDate.substring(0,7).equals("2014-01") ){
				return 0; //pass to partition 0
			}
			else if( accessDate.substring(0,7).equals("2014-02") ){
				return 1 % numReduceTasks; //pass to partition 1
			}
			else if( accessDate.substring(0,7).equals("2014-03") ){
				return 2 % numReduceTasks; //pass to partition 2
			}
			else if( accessDate.substring(0,7).equals("2014-04") ){
				return 3 % numReduceTasks; //pass to partition 3
			}
			else if( accessDate.substring(0,7).equals("2014-05") ){
				return 4 % numReduceTasks; //pass to partition 4
			}
			else if( accessDate.substring(0,7).equals("2014-06") ){
				return 5 % numReduceTasks; //pass to partition 5
			}
			else if( accessDate.substring(0,7).equals("2014-07") ){
				return 6 % numReduceTasks; //pass to partition 6
			}
			else if( accessDate.substring(0,7).equals("2014-08") ){
				return 7 % numReduceTasks; //pass to partition 7
			}
			else if( accessDate.substring(0,7).equals("2014-09") ){
				return 8 % numReduceTasks; //pass to partition 8
			}
			else if( accessDate.substring(0,7).equals("2014-10") ){
				return 9 % numReduceTasks; //pass to partition 9
			}
			else if( accessDate.substring(0,7).equals("2014-11") ){
				return 10 % numReduceTasks; //pass to partition 10
			}
			else if( accessDate.substring(0,7).equals("2014-12") ){
				return 11 % numReduceTasks; //pass to partition 11
			} else {
				return 12 % numReduceTasks; //pass to partition 12 (default case)
			}
		}
	}  
	//reduce class definition will not aggregate and will
	//simply group records by month of access request
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
		Job job = Job.getInstance(conf, "Server Access Analyzer");
		job.setNumReduceTasks(13); //will run with 13 reducers, one for each month and a default
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