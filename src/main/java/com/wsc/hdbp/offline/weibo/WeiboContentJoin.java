package com.wsc.hdbp.offline.weibo;

import java.io.IOException;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class WeiboContentJoin {

	/**/
	public static class ContentJoinMapper extends Mapper<Object,Text,Text,MapWritable>{
		
		
		/**
		 * the split value and its meaning as below
		 * index      meaning                remark
		 * 0          微博id                
		 * 1          用户id
		 * 2          是否为转发微博           true || false
		 * 3          原微博Id
		 * 4          此微博的转发数
		 * 5          评论数
		 * 6          喜欢数
		 * 7          微博创建的时间戳          
		 * 8          此微博来自的地方
		 * 9          微博内容
		 * 10         评价列表
		 * 11         转发列表                评价列表和转发列表的每一条信息以中文';'作为分隔符
           */
		@Override
		public void map(Object obj,Text value,Context context) 
				throws IOException, InterruptedException{
			/**
			 * use the Chinese symbol '，' to split the value string into 10 pieces
			 * the first 9 pieces point to the content below mentioned
			 * the 10th piece contains the content of the Weibo and replies as well as the transmit list
			 * */
			String[] splitValue = value.toString().split("，",12);
			/*invalid value,discard it*/
			if(splitValue.length != 12){
				return ;
			}
			
			String key = splitValue[1];
			if(!key.matches("\\d{10}")){
				return;    //invalid key ,discard 
			}
			
			MapWritable map = new MapWritable();
			
			map.put(new Text("content"), new Text(WeiboSplitter.pureContent(splitValue[9])));
			map.put(new Text("replies"), new Text(WeiboSplitter.pureContent(splitValue[10])));
			map.put(new Text("repeats"), new Text(WeiboSplitter.pureContent(splitValue[11])));
			
			context.write(new Text(key), map);
		}
	}
	
	public static class ContentReducer extends Reducer<Text,MapWritable,Text,Text>{
		@Override
		public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException{
			StringBuilder sb_content = new StringBuilder();
			StringBuilder sb_replies = new StringBuilder();
			StringBuilder sb_repeat = new StringBuilder();
			/*combine WeiBo by person*/
			for(MapWritable value:values){
				sb_content.append(value.get(new Text("content")));
				sb_replies.append(value.get(new Text("replies")));
				sb_repeat.append(value.get(new Text("repeats")));
			}
			String result = sb_content.toString().replaceAll("[^a-zA-Z\u4E00-\u9FA5+]"," ") +"|"
					+sb_replies.toString().replaceAll("[^a-zA-Z\u4E00-\u9FA5+]"," ")+"|"
					+sb_repeat.toString().replaceAll("[^a-zA-Z\u4E00-\u9FA5+]"," ");
			context.write(key, new Text(result));
		}
	}
	
}
