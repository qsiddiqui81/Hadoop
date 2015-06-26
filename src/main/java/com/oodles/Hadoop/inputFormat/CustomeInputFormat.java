package com.oodles.Hadoop.inputFormat;

import java.io.IOException;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class CustomeInputFormat extends FileInputFormat<MyKey, MyValue>{

	@Override
	public RecordReader<MyKey, MyValue> createRecordReader(InputSplit split,
			TaskAttemptContext context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		return new MyRecordReader();
	}
}