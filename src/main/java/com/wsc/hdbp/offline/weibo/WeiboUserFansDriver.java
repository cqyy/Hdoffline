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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.wsc.hdbp.offline.outputformat.RedisOutputFormatForSet;

public class WeiboUserFansDriver extends Configured implements Tool {

	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = new Configuration();
		Properties properties = new Properties();
		properties.loadFromXML(
				new FileInputStream(
						new File("./conf/weibo.io.dir.xml")));
		//String redisHost = properties.getProperty("redis.host");
		String indir = properties.getProperty("weibo.user.output");
		indir +=  indir.endsWith("/")?"*":"/*";
		Path inPath = new Path(indir);
		
		Job job = new Job(conf,"WeiboUserFans");
		job.setJarByClass(this.getClass());
		job.setMapperClass(WeiboUserFans.FansMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputFormatClass(RedisOutputFormatForSet.class);
		//RedisOutputFormatForSet.setRedisHosts(job, redisHost);
		RedisOutputFormatForSet.setRedisSelection(job,RedisSelection.FAN_SHIP.index());
		FileInputFormat.addInputPath(job, inPath);
		
		int status = job.waitForCompletion(true)?0:1;
		return status;
	}
	
	public static void main(String[] args) throws Exception{
		int exitCode = ToolRunner.run(new WeiboUserFansDriver(), args);
		System.exit(exitCode);
	}
}
