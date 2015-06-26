package com.oodles.Hadoop;

import java.io.IOException;
import java.util.Date;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class Counter {
	
	public static enum MONTH{
		DEC,
		JAN,
		FEB
	};
	
	public static class Map extends Mapper<LongWritable, Text, Text, Text>{
		private Text out = new Text();
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String line = value.toString();
        	String[]  strts = line.split(",");
        	long lts = Long.parseLong(strts[1]);
        	Date time = new Date(lts);
        	int m = time.getMonth();
        	
        	if(m==11){
        		context.getCounter(MONTH.DEC).increment(10);	
        	}
        	if(m==0){      	  	
      	  		context.getCounter(MONTH.JAN).increment(20);
        	}
        	if(m==1){
      	  		context.getCounter(MONTH.FEB).increment(30);
        	}
      	  	out.set("success");
      	  context.write(out,out);
		}
	}
	
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Path outputPath = new Path("/home/oodles/output");
		outputPath.getFileSystem(conf).delete(outputPath);
		
		Job job = new Job(conf,"counter data set");
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapperClass(Map.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path("/home/oodles/input"));
		FileOutputFormat.setOutputPath(job, outputPath);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
		Counters counters = job.getCounters();
	    
	    
	}
}
