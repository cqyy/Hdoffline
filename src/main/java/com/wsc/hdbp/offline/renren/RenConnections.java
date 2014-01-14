package com.wsc.hdbp.offline.renren;

import com.wsc.hdbp.offline.outputformat.RedisOutputFormatForSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

public class RenConnections {
	public final static class GroupMapper extends
			Mapper<LongWritable, Text, Text, Text> {

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String oneItem = line.substring(0, line.length() - 3);
			String[] col = oneItem.split(";", 2);

			String userId = col[0];
			Set<String> followSet = new HashSet<String>();

			for (String follow : col[1].split(",")) {
				if (follow.length() > 5) {
					followSet.add(follow.trim());
				} else {
					continue;
				}
			}

			// 粉丝以逗号分隔
			for (String follow : followSet) {
				context.write(new Text(userId), new Text(follow));
			}
		}
	}

	public static void main(String[] args) {
		InputStream in = RenConnections.class.getClassLoader()
				.getResourceAsStream("redis.properties");

		if (args.length != 1) {
			System.out.println("ERROR: Wrong number of parameters: "
					+ args.length + " instead of 1.");
			return;
		}
		String input = args[0];

		Properties properties = new Properties();
		try {
			properties.load(in);
		} catch (IOException exception) {
			exception.printStackTrace();
		}
		// 读取配置文件中的redis服务器IP
		String redisHost = properties.getProperty("redis.server.host");

		Path inputPath = new Path(input);
		try {
			Job followShipJob = new Job(new Configuration(),
					"Build Renren Connections In Redis");
			followShipJob.setMapperClass(GroupMapper.class);
			followShipJob.setJarByClass(RenConnections.class);

			followShipJob.setInputFormatClass(TextInputFormat.class);

			followShipJob.setOutputFormatClass(RedisOutputFormatForSet.class);
			RedisOutputFormatForSet.setRedisHosts(followShipJob, redisHost);
			RedisOutputFormatForSet.setRedisSelection(followShipJob, 7);// 好友库

			followShipJob.setOutputKeyClass(Text.class);
			followShipJob.setOutputValueClass(Text.class);

			TextInputFormat.addInputPath(followShipJob, inputPath);

			System.exit(followShipJob.waitForCompletion(false) ? 0 : 1);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
}
