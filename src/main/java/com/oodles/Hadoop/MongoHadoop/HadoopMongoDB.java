package com.oodles.Hadoop.MongoHadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.ToolRunner;
import org.bson.BSONObject;

import com.mongodb.hadoop.MongoConfig;
import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.util.MapredMongoConfigUtil;
import com.mongodb.hadoop.util.MongoConfigUtil;
import com.mongodb.hadoop.util.MongoTool;


public class HadoopMongoDB extends MongoTool{
	public static class Map extends Mapper<Object, BSONObject, Text, IntWritable>{
		public void map(final Object key, final BSONObject value, final Context context) throws IOException, InterruptedException{
			System.out.println(value);
			/**
			 * write your mapper logic
			 */
			context.write(new Text(), new IntWritable(1));	
		}
	}
	
	public static class Reduce extends Reducer<Text,IntWritable,Text,IntWritable>{
		public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException{
			/**
			 * write your reducer logic
			 */
			context.write(new Text(), new IntWritable(1));
		}
	}
	
	public HadoopMongoDB(){
		Configuration conf = new Configuration();
		MongoConfig mongoConfig = new MongoConfig(conf);
		setConf(conf);
		if (MongoTool.isMapRedV1()) {
            MapredMongoConfigUtil.setInputFormat(getConf(), com.mongodb.hadoop.mapred.MongoInputFormat.class);
            MapredMongoConfigUtil.setOutputFormat(getConf(), com.mongodb.hadoop.mapred.MongoOutputFormat.class);
        } else {
            MongoConfigUtil.setInputFormat(getConf(), MongoInputFormat.class);
            MongoConfigUtil.setOutputFormat(getConf(), MongoOutputFormat.class);
        }
		mongoConfig.setInputFormat(MongoInputFormat.class);
		mongoConfig.setInputURI("mongodb://localhost:27017/samepinch.users");
		mongoConfig.setMapper((Class<? extends Mapper>) Map.class);
		mongoConfig.setReducer(Reduce.class);
		mongoConfig.setMapperOutputKey(Text.class);
		mongoConfig.setMapperOutputValue(IntWritable.class);
		mongoConfig.setOutputKey(Text.class);
		mongoConfig.setOutputValue(IntWritable.class);
		mongoConfig.setOutputURI("mongodb://localhost:27017/dbName.outputCollectionName");
		mongoConfig.setOutputFormat(MongoOutputFormat.class);
	}
	
	public static void main(String[] args) throws Exception {
		 System.exit(ToolRunner.run(new HadoopMongoDB(), args));
	}
}
