package com.wsc.hdbp.offline.renren;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;

public class RenStatus1 {
	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {
		private Text user = new Text();
		private Text status = new Text();

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String[] col = value.toString().split(",");

			String userId = col[1].trim();
			String content = col[10].trim();
			user.set(userId);
			status.set(content);
			output.collect(user, status);
		}
	}

	public static class Reduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String str = "";
			while (values.hasNext()) {
				str += values.next().toString() + " ";
			}
			output.collect(key, new Text(str));
		}
	}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(RenStatus1.class);
		conf.setJobName("Renren单个用户的所有状态整合");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		// String[] args1 = { "/home/elliot/weibo.txt.0", "/tmp/KeyWords/tmp" };
		/*
		 * DataInfo d = new DataInfo("192.168.1.205", 6001); String[] files =
		 * d.getFlashData("SinaWeibo", "weibo.txt"); String inputs = ""; for
		 * (String file : files) { inputs += ",hdfs://server:8020/crawler/" +
		 * file; } inputs = inputs.substring(1);
		 */
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		JobClient.runJob(conf);
	}

}
