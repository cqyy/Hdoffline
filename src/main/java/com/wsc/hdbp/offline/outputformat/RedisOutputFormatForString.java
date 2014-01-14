package com.wsc.hdbp.offline.outputformat;

import com.wsc.hdbp.util.JedisConnection;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.log4j.Logger;
import redis.clients.jedis.Jedis;

import java.io.IOException;

/**
 * 粉丝表: 数据库1 key=userid value=set(粉丝列表)
 * 
 * @author Elliot
 */
public class RedisOutputFormatForString extends OutputFormat<Text, Text> {

	public static final String REDIS_HOST_CONF = "mapred.redis.host";
	public static final String REDIS_HOST_SELECT = "mapred.redis.selection";

	public static void setRedisHosts(Job job, String redisHost) {
		job.getConfiguration().set(REDIS_HOST_CONF, redisHost);
	}

	public static void setRedisSelection(Job job, int selection) {
		job.getConfiguration().setInt(REDIS_HOST_SELECT, selection);
	}

	public class RedisSetRecordWriter extends RecordWriter<Text, Text> {

		private final Logger LOG = Logger.getLogger(RedisSetRecordWriter.class);
		private Jedis jedis = null;

		public RedisSetRecordWriter(JobContext job) {
			String redisHost = job.getConfiguration().get(REDIS_HOST_CONF);
			int redisSelection = Integer.parseInt(job.getConfiguration().get(
					REDIS_HOST_SELECT));
			LOG.info("Connecting to Redis at " + redisHost);
			jedis = JedisConnection.getJedisConnection();
			// 粉丝列表位于数据库1
			LOG.info("Select database #" + redisSelection);
			jedis.select(redisSelection);
		}

		@Override
		public void write(Text key, Text value) throws IOException,
				InterruptedException {
			// 以集合方式存储粉丝列表
			jedis.set(key.toString(), value.toString());
		}

		@Override
		public void close(TaskAttemptContext context) throws IOException,
				InterruptedException {
			if (jedis != null)
				JedisConnection.dropJedisConnection(jedis);
		}
	}

	@Override
	public void checkOutputSpecs(JobContext job) throws IOException,
			InterruptedException {
//		String hosts = job.getConfiguration().get(REDIS_HOST_CONF);
//
//		if (hosts == null || hosts.isEmpty()) {
//			throw new IOException(REDIS_HOST_CONF
//					+ " is not set in configuration.");
//		}
	}

	@Override
	public OutputCommitter getOutputCommitter(TaskAttemptContext context)
			throws IOException, InterruptedException {
		return (new NullOutputFormat<Text, Text>()).getOutputCommitter(context);
	}

	@Override
	public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext job)
			throws IOException, InterruptedException {
		return new RedisSetRecordWriter(job);
	}
}
