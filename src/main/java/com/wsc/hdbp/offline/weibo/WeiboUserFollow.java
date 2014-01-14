package com.wsc.hdbp.offline.weibo;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WeiboUserFollow {
	public static class FollowMapper extends Mapper<Object,Text,Text,Text>{
		
		@Override
		public void map(Object obj,Text value,Context context) throws IOException, InterruptedException{
			String[] values = value.toString().split("\\s",2);
			if(values.length!=2){
				return;
			}
			Text userId = new Text(values[0].trim());
			String fans = values[1].substring(values[1].indexOf("|")+1,values[1].length());
			fans = fans.replaceAll("；",",");
			context.write(userId, new Text(fans));
		}
	}
}
