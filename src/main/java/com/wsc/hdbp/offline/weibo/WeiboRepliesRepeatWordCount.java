package com.wsc.hdbp.offline.weibo;

import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import com.wsc.hdbp.util.IKAnalyzer;

/**
 * @author YuanYe
 */
public class WeiboRepliesRepeatWordCount {
	public static class RedisKeyWordsMapper extends
			Mapper<Object, Text, Text, MapWritable> {
		private Text outkey = new Text();

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] delem = value.toString().split("\t",2);
			if (delem.length != 2) {
				return;
			}
			String userId = delem[0];
			String tempContent = delem[1];
			String joinedContent = tempContent.substring(
					tempContent.indexOf("|"));
			
			Map<String, Integer> wordFrequency = IKAnalyzer
					.getWordFrequency(joinedContent);
			MapWritable mapWritable = new MapWritable();
			for (Map.Entry<String, Integer> entry : wordFrequency.entrySet()) {
				mapWritable.put(new Text(entry.getKey()), new Text(entry
						.getValue().toString()));
			}
			outkey = new Text(userId);
			
			context.write(outkey, mapWritable);
		}
	}


}
