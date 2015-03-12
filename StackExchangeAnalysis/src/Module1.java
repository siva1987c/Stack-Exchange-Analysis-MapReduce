// The purpose of the program is to find the average number of answers per question.
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Module1 {

	public static class Mapper1 extends Mapper<LongWritable, Text, Text, IntWritable> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			int count  = 0;
			String line = value.toString();
			String[] splitLine = line.split("\"");
			String postTypeId = splitLine[3];
			int answerCountStringIndex = -1;
			if(postTypeId.contains("1")){
				for(int i = splitLine.length - 1; i >= 0 ; i--) {
					if(splitLine[i].contains("AnswerCount=")) {
						answerCountStringIndex = i; 
						break;
					}
				}
				if(answerCountStringIndex != -1) {
					count = Integer.parseInt(splitLine[answerCountStringIndex + 1]);
					context.write(new Text("Question"), new IntWritable(count));
				}
			}
		}
	}
  
	public static class Reducer1 extends Reducer<Text,IntWritable,Text,FloatWritable> {
		Text word = new Text("Average");
		static float questionTotal = 0, answerTotal = 0;
		float average = 0;
	  
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			for(IntWritable val : values) {
					questionTotal = questionTotal + 1;
					answerTotal = answerTotal + val.get();
			}
			average = answerTotal / questionTotal;
			context.write(word, new FloatWritable(average));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Question 1");
		job.setJarByClass(Module1.class);
		job.setMapperClass(Mapper1.class);
		job.setReducerClass(Reducer1.class);
	
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
	
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
