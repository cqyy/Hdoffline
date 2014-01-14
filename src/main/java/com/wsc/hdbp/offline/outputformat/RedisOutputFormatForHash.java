package com.wsc.hdbp.offline.outputformat;

import com.wsc.hdbp.util.JedisConnection;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * 关键词表: 数据库2 key=userid value=hash(关键词词频)
 * 
 * @author Elliot
 */
public class RedisOutputFormatForHash extends OutputFormat<Text, MapWritable> {

	public final static String REDIS_HOST_CONF = "mapred.redis.host";
	public static final String REDIS_HOST_SELECT = "mapred.redis.selection";

	public static void setRedisHosts(Job job, String host) {
		job.getConfiguration().set(REDIS_HOST_CONF, host);
	}

	public static void setRedisSelection(Job job, int selection) {
		job.getConfiguration().setInt(REDIS_HOST_SELECT, selection);
	}

	public class RedisHashRecordWriter extends RecordWriter<Text, MapWritable> {

		private Jedis jedis = null;

		public RedisHashRecordWriter(JobContext job) {

			int redisSelection = Integer.parseInt(job.getConfiguration().get(
					REDIS_HOST_SELECT));
			// LOG.info("Connecting to Redis at " + redisHost +
			// " db="+redisSelection);
			jedis = JedisConnection.getJedisConnection();
			jedis.select(redisSelection);
		}

		@Override
		public void write(Text key, MapWritable value) throws IOException,
				InterruptedException {

			// Write the key/value pair
			HashMap<String, String> wf = new HashMap<String, String>();
			for (Map.Entry<Writable, Writable> entry : value.entrySet()) {
				wf.put(entry.getKey().toString(),
						entry.getValue().toString());
			}
			try {
				if (jedis.exists(key.toString())) {
					Map<String, String> beforeMap = jedis.hgetAll(key
							.toString());
					for (String k : beforeMap.keySet()) {
						if (wf.containsKey(k)) {
							try {
								int f1 = Integer.parseInt(beforeMap.get(k));
								int f2 = Integer.parseInt(wf.get(k));
								wf.put(k, Integer.toString(f1 + f2));
							} catch (NumberFormatException e) {
								continue;
							}
						} else {
							wf.put(k, beforeMap.get(k));
						}
					}
				}
			} catch (redis.clients.jedis.exceptions.JedisConnectionException e) {
			}
			try {
				jedis.hmset(key.toString(), wf);
			} catch (redis.clients.jedis.exceptions.JedisDataException e) {
				//System.out.println(key.toString());
				//System.out.println(wf);
			}
		}

		@Override
		public void close(TaskAttemptContext context) throws IOException,
				InterruptedException {
			// disconnect redis instance
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

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.apache.hadoop.mapreduce.OutputFormat#getOutputCommitter(org.apache
	 * .hadoop.mapreduce.TaskAttemptContext)
	 */
	@Override
	public OutputCommitter getOutputCommitter(TaskAttemptContext context)
			throws IOException, InterruptedException {
		return (new NullOutputFormat<Text, Text>()).getOutputCommitter(context);
	}

	@Override
	public RecordWriter<Text, MapWritable> getRecordWriter(
			TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new RedisHashRecordWriter(context);
	}

}
