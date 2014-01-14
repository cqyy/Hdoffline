package com.wsc.hdbp.offline.weibo;

import java.io.File;
import java.io.FileInputStream;
import java.net.URI;
import java.util.Properties;

import com.wsc.hdbp.offline.outputformat.RedisOutputFormatForHash;
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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.wsc.hdbp.offline.weibo.WeiboRepliesRepeatCount.RRCountMapper;
import com.wsc.hdbp.offline.weibo.WeiboRepliesRepeatCount.RRReducer;


public class WeiboRepliesRepeatCountDriver extends Configured implements Tool{

	@Override
	public int run(String[] arg0) throws Exception {
		Properties properties = new Properties();
		properties.loadFromXML(
				new FileInputStream(
						new File("./conf/weibo.io.dir.xml")));
		String inputStr = properties.getProperty("weibo.contentJoin.output");
		inputStr += inputStr.endsWith("/")?"*":"/*";
		String tempdir = properties.getProperty("weibo.comment_repeat.count.temp");
		String uri = properties.getProperty("hdfs.uri");
		
		Path input = new Path(inputStr);
		Path tempPath = new Path(tempdir);
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI(uri),conf);

		if(fs.exists(tempPath)){
			fs.delete(tempPath, true);
		}
		
		Job job1 = new Job(conf,"WeiboCommentRepeatCount_Phrase_1");
		
		job1.setJarByClass(WeiboRepliesRepeatCount.class);
		job1.setMapperClass(RRCountMapper.class);
		job1.setReducerClass(RRReducer.class);
		
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(MapWritable.class);
		
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job1, input);
		FileOutputFormat.setOutputPath(job1, tempPath);
		
		job1.waitForCompletion(true);


        Job job2 = new Job(conf,"WeiboCommentRepeatCount_Phrase_2");
        job2.setJarByClass(WeiboRepliesRepeatCount.class);
        job2.setMapperClass(WeiboRepliesRepeatCount.RRCountOutMapper.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(MapWritable.class);

        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(RedisOutputFormatForHash.class);
        FileInputFormat.addInputPath(job2,tempPath);
        RedisOutputFormatForHash.setRedisSelection(job2,RedisSelection.REPEAT_REPLY_COUNT.index());

        job2.waitForCompletion(true);

		return 0;
	}
	
	public static void main(String[] args) throws Exception{
		int exitCode = ToolRunner.run(new WeiboRepliesRepeatCountDriver(),args);
		System.exit(exitCode);
	}

}
