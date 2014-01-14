package com.wsc.hdbp.offline.renren;

import com.wsc.hdbp.offline.outputformat.RedisOutputFormatForHash;
import com.wsc.hdbp.util.IKAnalyzer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Redis#8
 * 
 * @author elliot
 */
public class RenStatus2 {

	public static class RedisKeyWordsMapper extends
			Mapper<Object, Text, Text, MapWritable> {
		private Text outkey = new Text();

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] delem = value.toString().split("\t");
			if (delem.length < 2) {
				return;
			}
			String userId = delem[0];
			String joinedContent = delem[1];
			HashMap<String, Float> wordFrequency = IKAnalyzer
					.weiboWord(joinedContent);
			MapWritable vector = new MapWritable();
			for (Map.Entry<String, Float> entry : wordFrequency.entrySet()) {
				vector.put(new Text(entry.getKey()), new Text(entry.getValue()
						.toString()));
			}
			outkey = new Text(userId);
			context.write(outkey, vector);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		InputStream in = RenStatus2.class.getClassLoader().getResourceAsStream(
				"redis.properties");
		Properties properties = new Properties();
		try {
			properties.load(in);
		} catch (IOException exception) {
			exception.printStackTrace();
		}
		// 读取配置文件中的redis服务器IP
		String redisHost = properties.getProperty("redis.server.host");

		// 第二阶段 - 词频分析
		Job job2 = new Job(conf, "分析Renren文本关键词频率向Redis输出");
		job2.setJarByClass(RenStatus2.class);
		job2.setMapperClass(RedisKeyWordsMapper.class);
		job2.setNumReduceTasks(0);
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(RedisOutputFormatForHash.class);
		FileInputFormat.addInputPath(job2, new Path(args[0]));
		RedisOutputFormatForHash.setRedisHosts(job2, redisHost);
		RedisOutputFormatForHash.setRedisSelection(job2, 8);// #8
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(MapWritable.class);
		int code = job2.waitForCompletion(true) ? 0 : 2;
		System.exit(code);
	}

}
