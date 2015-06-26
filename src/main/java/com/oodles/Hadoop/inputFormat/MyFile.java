package com.oodles.Hadoop.inputFormat;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MyFile {
	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Path outputPath = new Path("/home/oodles/output");
		outputPath.getFileSystem(conf).delete(outputPath);
		

		Job job = new Job(conf,"custom input formate");
		job.setJarByClass(MyFile.class);
		job.setJobName("CustomTest");
		job.setNumReduceTasks(0);
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setInputFormatClass(CustomeInputFormat.class);

		FileInputFormat.addInputPath(job, new Path("/home/oodles/input"));
		FileOutputFormat.setOutputPath(job, outputPath);

		job.waitForCompletion(true);
	}
}
