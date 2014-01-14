package com.wsc.hdbp.offline.weibo;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class WeiboUserFilterDriver extends Configured implements Tool {

	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = new Configuration();
		Properties properties = new Properties();
		properties.loadFromXML(
				new FileInputStream(
						new File("./conf/weibo.io.dir.xml")));
		String uri = properties.getProperty("hdfs.uri");
		Path inPath = new Path(properties.getProperty("weibo.user.input"));
		Path outPath = new Path(properties.getProperty("weibo.user.output"));
		
		Job job = new Job(conf,"WeiboInfoFilter");
		job.setJarByClass(this.getClass());
		job.setMapperClass(WeiboUserFilter.InfoFilterMapper.class);
		job.setReducerClass(WeiboUserFilter.InfoFilterReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(WeiboUserFilter.TextArrayWritable.class);
		FileInputFormat.addInputPath(job, inPath);
		FileOutputFormat.setOutputPath(job, outPath);
		
		int status = job.waitForCompletion(true)?0:1;
		
		
		//complete successful , rename files dealt
		if(status == 0){
			FileMarker.makeMarker(inPath, uri);
		}
		
		return status;
	}
	public static void main(String[] args) throws Exception{
		int exitCode = ToolRunner.run(new WeiboUserFilterDriver(), args);
		System.exit(exitCode);
	}
}
