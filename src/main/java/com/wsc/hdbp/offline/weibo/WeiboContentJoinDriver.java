package com.wsc.hdbp.offline.weibo;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.wsc.hdbp.offline.weibo.WeiboContentJoin.ContentJoinMapper;
import com.wsc.hdbp.offline.weibo.WeiboContentJoin.ContentReducer;

/**
 * <p>run the word MapReduce</p>
 * @author YuanYe
 */

public class WeiboContentJoinDriver extends Configured implements Tool{
	
	
	
	public static void main(String[] args) throws Exception{
		int exitCode = ToolRunner.run(new WeiboContentJoinDriver(),args);
		System.exit(exitCode);
	}

	@Override
	public int run(String[] arg0) throws Exception {
		Properties properties = new Properties();
		properties.loadFromXML(
				new FileInputStream(
						new File("./conf/weibo.io.dir.xml")));
		
		String in = properties.getProperty("weibo.contentJoin.input");
		String hdfsURI = properties.getProperty("hdfs.uri");
		Path inPath = new Path(in);
		Path outPath = new Path(properties.getProperty("weibo.contentJoin.output"));
		
		Configuration conf = new Configuration();
		Job job = new Job(conf, "WeiboContentJoin");
		
		job.setJarByClass(WeiboContentJoin.class);
		job.setMapperClass(ContentJoinMapper.class);
		job.setReducerClass(ContentReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(MapWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, inPath);
		FileOutputFormat.setOutputPath(job, outPath);

		int status = job.waitForCompletion(true)?0:1;
		
		//complete successful , rename files dealt
		if(status == 0){
			FileMarker.makeMarker(inPath, hdfsURI);
		}
		
		return 0;
	};
}
