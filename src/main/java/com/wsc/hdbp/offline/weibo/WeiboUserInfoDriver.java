package com.wsc.hdbp.offline.weibo;

import java.io.File;
import java.io.FileInputStream;
import java.net.URI;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

import com.wsc.hdbp.offline.outputformat.RedisOutputFormatForHashCover;
import com.wsc.hdbp.offline.weibo.WeiboUserInfo.CompareTimestampReducer;
import com.wsc.hdbp.offline.weibo.WeiboUserInfo.TokenizerMapper;
import com.wsc.hdbp.offline.weibo.WeiboUserInfo.TokenizerMapper2;

public class WeiboUserInfoDriver extends Configured implements Tool {

	@Override
	public int run(String[] arg0) throws Exception{
		
		Properties properties = new Properties();
		properties.loadFromXML(
				new FileInputStream(
						new File("./conf/weibo.io.dir.xml")));
		
		String uri = properties.getProperty("hdfs.uri");	
		Path inputPath = new Path(properties.getProperty("weibo.info.input"));
		Path outPath = new Path(properties.getProperty("weibo.info.output"));

		Configuration conf = new Configuration();
		// 注意此处的URI作为是作为下面tmp的上级目录的（tmp是相对路径的情况下）
		// 如果tmp是绝对路径
		FileSystem fs = FileSystem.get(new URI("hdfs://server:8020/"), conf);
		if (fs.exists(outPath)) {
			fs.delete(outPath, true);
		}
		
		boolean status = false;
		
		// 第一步的相关设置
		Job job1 = new Job(conf, "Build UserInfo In Redis#1");
		job1.setMapperClass(TokenizerMapper.class);
		job1.setReducerClass(CompareTimestampReducer.class);
		job1.setJarByClass(WeiboUserInfo.class);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);
		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job1, inputPath);
		FileOutputFormat.setOutputPath(job1, outPath);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		// 等待job1完成,因为job2依赖于job1
		status = job1.waitForCompletion(true);

		// 第二步的相关设置
		Job job2 = new Job(conf, "Build UserInfo In Redis#2");
		job2.setMapperClass(TokenizerMapper2.class);
		job2.setJarByClass(WeiboUserInfo.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(MapWritable.class);
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(RedisOutputFormatForHashCover.class);
		//RedisOutputFormatForHashCover.setRedisHosts(job2, redisHost);
		RedisOutputFormatForHashCover.setRedisSelection(job2, RedisSelection.USER_INFO.index());
		TextInputFormat.addInputPath(job2, outPath);
		// 等待job2结束
		status &= job2.waitForCompletion(true);
		// 删除临时路径
		if (fs.exists(outPath)) {
			fs.delete(outPath, true);
		}
		
		//complete successful , rename files dealt
		if(status){
			FileMarker.makeMarker(inputPath, uri);
		}
		
		return 0;
	}
	
	public static void main(String[] args) throws Exception{
		int exitCode = new WeiboUserInfoDriver().run(args);
		
		System.exit(exitCode);
	}
}
