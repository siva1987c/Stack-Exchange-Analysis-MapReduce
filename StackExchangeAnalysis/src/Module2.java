// The purpose of the program is to find the average score per accepted answer and also for the answers which were not marked as accepted.
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

public class Module2 {

	public static class Mapper2 extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
        
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] splitLine = line.split("\"");
			String postTypeId = splitLine[3];
			int acceptedIdStringIndex = -1;
			int scoreIndex = -1;
			if(postTypeId.contains("1")){
				for(int i = 0; i < splitLine.length ; i++) {
					if(splitLine[i].contains("AcceptedAnswerId")) {
						acceptedIdStringIndex = i; 
						break;
					}
				}
				if(acceptedIdStringIndex != -1) {
					int acceptedId = Integer.parseInt(splitLine[acceptedIdStringIndex + 1]);
					context.write(new IntWritable(acceptedId), new IntWritable(0));
				}
			}
			else
			if(postTypeId.contains("2")) {
				for(int i = 0; i < splitLine.length ; i++) {
					if(splitLine[i].contains("Score")) {
						scoreIndex = i; 
						break;
					}
				}
				if(scoreIndex != -1) {
					int scoreValue = Integer.parseInt(splitLine[scoreIndex + 1]);
					context.write(new IntWritable(Integer.parseInt(splitLine[1])), new IntWritable(scoreValue));
				}
			}
        }
	}
      
	public static class Reducer2 extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        	int count = 0, sum = 0;
			for(IntWritable val : values) {
				sum += val.get();
				count += 1;
			}
			if(count >= 2)
        		context.write(new IntWritable(1), new IntWritable(sum));
			else
				context.write(new IntWritable(0), new IntWritable(sum));
        }       
    }

	public static class MapperTwo extends Mapper<LongWritable, Text, IntWritable, IntWritable>{
          
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			   
  			String line = value.toString();
  			String[] splitLine = line.split("\\s+");
  			context.write(new IntWritable(Integer.parseInt(splitLine[0].trim())), new IntWritable(Integer.parseInt(splitLine[splitLine.length - 1].trim())));
  			
		}
	}
        
	public static class ReducerTwo extends Reducer<IntWritable,IntWritable,Text,FloatWritable> {

		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			float totalSum = 0, count = 0;
  			for(IntWritable val : values) {
  					totalSum += val.get();
  					count += 1;
  			}
  			if(key.get() == 1)
  				context.write(new Text("Accepted Answers Average: "), new FloatWritable(totalSum / count));
  			else
  				context.write(new Text("Unaccepted Answers Average: "), new FloatWritable(totalSum / count));
		}       
	}
        
	public static int main(String[] args) throws Exception {
		Configuration conf = new Configuration();
    	Job job = new Job(conf, "Question 2");
    	job.setJarByClass(Module2.class);
    	job.setMapperClass(Mapper2.class);
    	job.setReducerClass(Reducer2.class);

   		job.setOutputFormatClass(TextOutputFormat.class);
   	    FileInputFormat.addInputPath(job, new Path(args[0]));
    		
   	    FileOutputFormat.setOutputPath(job, new Path("/bigd18/temp/"));
   	    job.setOutputKeyClass(IntWritable.class);
   	    job.setOutputValueClass(IntWritable.class);
   	    job.submit();
   	    job.waitForCompletion(true);
    	    
   	    Configuration conf2 = new Configuration();
   	    Job job2 = new Job(conf2, "Question 2 Part 2");
   	    
   	    job2.setJarByClass(Question2.class);
   	    job2.setMapperClass(MapperTwo.class);
   	    job2.setReducerClass(ReducerTwo.class);
   		job2.setOutputFormatClass(TextOutputFormat.class);
   	    FileInputFormat.addInputPath(job2, new Path("/bigd18/temp/"));
    		
   	    FileOutputFormat.setOutputPath(job2, new Path(args[1]));
   	    job2.setOutputKeyClass(IntWritable.class);
   	    job2.setOutputValueClass(IntWritable.class);

   	    job2.submit();
   	    return job2.waitForCompletion(true) ? 0 : 1;
	}
}