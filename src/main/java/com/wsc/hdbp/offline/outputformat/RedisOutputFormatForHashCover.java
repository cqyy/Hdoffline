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
public class RedisOutputFormatForHashCover extends
		OutputFormat<Text, MapWritable> {

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
			Map<String,String> old = new HashMap<String,String>();
			old = jedis.hgetAll(key.toString());
			
			// Write the key/value pair
			HashMap<String, String> wf = new HashMap<String, String>();
			for (Map.Entry<Writable, Writable> entry : value.entrySet()) {
				wf.put(((Text) (entry.getKey())).toString(),
						((Text) (entry.getValue())).toString());
			}
			
			//compare if data changed
			boolean changed = false;
			for(Map.Entry<String, String> entry : old.entrySet()){
				if(!entry.getValue().equals(wf.get(entry.getKey()))){
					changed = true;
				};
			}
			if(!changed){
				//no change,do nothing
				return;
			}
			try {
				jedis.hmset(key.toString(), wf);
				jedis.hmset(key.toString()+"_old", old);
			} catch (redis.clients.jedis.exceptions.JedisDataException e) {
				System.out.println(key.toString());
				System.out.println(wf);
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
