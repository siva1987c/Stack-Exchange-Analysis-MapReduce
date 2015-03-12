// Purpose of the program is to find number of answers per userID
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Module3 {

	public static class Mapper3 extends Mapper<LongWritable, Text, IntWritable, IntWritable>{

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			int userId = 0;
			String line = value.toString();
			String[] splitLine = line.split("\"");
			String postTypeId = splitLine[3];
			int userIdStringIndex = -1;
			if(postTypeId.contains("2")){
				for(int i = splitLine.length - 1; i >= 0; i--) {
					if(splitLine[i].contains("OwnerUserId")) {
						userIdStringIndex = i;
						break;
					}
				}
				if(userIdStringIndex != -1) {
					userId = Integer.parseInt(splitLine[userIdStringIndex + 1]);
					context.write(new IntWritable(userId), new IntWritable(1));
				}
			}
    	}
	}
  
	public static class Reducer3 extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {

		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int totalAnswers = 0;
			for(IntWritable val : values) {
				totalAnswers += val.get();
			}
			context.write(key, new IntWritable(totalAnswers));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "Question 3");
		job.setJarByClass(Module3.class);
		job.setMapperClass(Mapper3.class);
		job.setReducerClass(Reducer3.class);

		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
	
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
