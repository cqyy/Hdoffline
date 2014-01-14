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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.wsc.hdbp.offline.outputformat.RedisOutputFormatForHash;
import com.wsc.hdbp.offline.weibo.WeiboContentWordCount.RedisKeyWordsMapper;

public class WeiboContentWordCountDriver extends Configured implements Tool {

	
	
	@Override
	public int run(String[] arg0) throws Exception {
		Properties properties = new Properties();
		properties.loadFromXML(
				new FileInputStream(
						new File("./conf/weibo.io.dir.xml")));
		String inputStr = properties.getProperty("weibo.contentJoin.output");
		inputStr += inputStr.endsWith("/")?"*":"/*";
		Path input = new Path(inputStr);
		
		Configuration conf = new Configuration();

		// 第二阶段 - 词频分析
		Job job2 = new Job(conf, "Content_To_Keywords");
		job2.setJarByClass(WeiboContentWordCount.class);
		job2.setMapperClass(RedisKeyWordsMapper.class);
		job2.setNumReduceTasks(0);
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(RedisOutputFormatForHash.class);
		FileInputFormat.addInputPath(job2, input);
		RedisOutputFormatForHash.setRedisSelection(job2, RedisSelection.WEIBO_CONTENT.index());
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(MapWritable.class);

		job2.submit();
		return 0;
	}
	
	public static void main(String[] args) throws Exception{
		ToolRunner.run(new WeiboContentWordCountDriver(), args);
	}

}
