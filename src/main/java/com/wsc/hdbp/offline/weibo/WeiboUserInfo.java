package com.wsc.hdbp.offline.weibo;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import com.google.gson.Gson;

/**
 * 统计统计后面会用到的字段，来源info.txt(中文逗号分隔)
 * 两个Job，第一步：比较时间戳，取最新；第二步：扔进Redis。
 * <p/>
 * <table>
 * <tr>
 * <td>用户ID</td>
 * <td>昵称</td>
 * <td>性别</td>
 * <td>粉丝数</td>
 * <td>关注人数</td>
 * <td>微博数</td>
 * <td>所在地</td>
 * <td>认证信息</td>
 * <td>个性域名(|)</td>
 * <td>个性标签(；)</td>
 * <td>个人简介</td>
 * <td>教育信息(；)</td>
 * <td>工作信息(；)</td>
 * <td>时间戳</td>
 * </tr>
 * </table>
 * 
 * @author Elliot
 */
public class WeiboUserInfo {

	public final static class TokenizerMapper extends
			Mapper<LongWritable, Text, Text, Text> {

		Gson gson = new Gson();
		Text outKey = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] values = value.toString().split("，");
			if(values.length < 15){
				return;
			}
			String[] valuesName = new String[] { "userid", "nick", "gender",
					"fanscount", "followcount", "weibocount", "location",
					"verifyinfo", "urls", "tags", "avatar", "introduction", "edu",
					"work", "timestamp", "usertype" };

			// used to get userType,if {@code lastValue} is timeStamp ,the userTpe
			// is empty,or it is userType
			String lastValue = values[values.length-1];
			Map<String, String> map = new HashMap<String, String>();
			int valuesItemCount = 16;
			if (lastValue.matches("\\d{13}")) {
				valuesItemCount--;
				map.put(valuesName[15], "");
			} else {
				map.put(valuesName[15], lastValue);
			}

			int tagsCount = values.length - valuesItemCount + 1; // tags 占有的分割数量
			// fill in values before the tags
			for (int i = 0; i < 9; i++) {
				map.put(valuesName[i], values[i]);
			}
			//verify three count values,if empty,set to '0'
			for(int i=3;i<6;i++){
				if(map.get(valuesName[i]).trim().isEmpty()){
					map.put(valuesName[i],"0");
				}
			}
			// fill in the tags
			String tags = "";
			for (int i = 0; i < tagsCount; i++) {
				tags += values[9 + i]+"；";
			}
			map.put(valuesName[9], tags);

			// fill left values
			for (int i = 0; i < 5; i++) {
				map.put(valuesName[10 + i], values[10 + i + tagsCount - 1]);
			}
			context.write(new Text(map.get(valuesName[0])), new Text(gson.toJson(map)));
		}
	}

	public final static class CompareTimestampReducer extends
			Reducer<Text, Text, Text, Text> {
		Map<String, String> map;
		long timestamp = 0L;
		Gson gson = new Gson();

		@SuppressWarnings("unchecked")
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text iterT : values) {
				Map<String, String> m = gson.fromJson(iterT.toString(),
						Map.class);
				if (Long.parseLong(m.get("timestamp")) > timestamp) {
					map = m;
				}
			}
			context.write(key, new Text(gson.toJson(map)));
		}
	}

	public final static class TokenizerMapper2 extends
			Mapper<Object, Text, Text, MapWritable> {
		Text outKey = new Text();
		Gson gson = new Gson();

		@SuppressWarnings("unchecked")
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] values = value.toString().split("\\s",2);
			if(values.length!=2){
				return;
			}
			String user = values[0];
			String mapStr = values[1];
			Map<String, String> m = gson.fromJson(mapStr, Map.class);
			MapWritable map = new MapWritable();
			for (String k : m.keySet()) {
				map.put(new Text(k), new Text(m.get(k)));
			}
			outKey.set(user);
			context.write(outKey, map);
		}
	}

}