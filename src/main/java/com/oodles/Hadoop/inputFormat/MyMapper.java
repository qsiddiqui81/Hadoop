package com.oodles.Hadoop.inputFormat;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<MyKey,MyValue,Text,Text>{
	protected void map(MyKey key,MyValue value,Context context) throws IOException, InterruptedException{
		 String sensor = key.getSensorType().toString();
         
         if(sensor.toLowerCase().equals("a")){
         	context.write(value.getValue1(),value.getValue2());
         }
	}
}
