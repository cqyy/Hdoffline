package com.wsc.hdbp.offline.weibo;

import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;



/**
 * <p>filter the user releationShip using the timeStamp to get the latest one</p>
 * <p>to the ones which are not latest,just discard</p>
 * @author YuanYe
 * */
public class WeiboUserFilter {
	
	public static class TextArrayWritable extends ArrayWritable{
		public TextArrayWritable(){
			super(Text.class);
		}
	}
	
	
	public static class InfoFilterMapper extends Mapper<Object,Text,Text,TextArrayWritable>{
		/**
		 * <p>split the value string to get userId,fans list, and follow list</p>
		 * <p>value string format:userId , follow1;follow2....followN , fans1;fans2;....fansN , timeStamp</p>
		 * @throws InterruptedException 
		 * @throws IOException 
		 * 
		 */
		@Override
		public void map(Object obj,Text value,Context context) throws IOException, InterruptedException{
			/*index to meaning
			 * 0 userId
			 * 1 follows list
			 * 2 fans list
			 * 3 timeStamp
			 * */
			String[] values = value.toString().split("，",4);
			
			///invalid value string
			if(values.length != 4){
				return ;
			}
			Text userId = new Text(values[0]);
			Text[] valueText = new Text[3];
			for(int i=0;i<3;i++){
				valueText[i] =  new Text(values[i+1]);
			}
			
			TextArrayWritable valueArray = new TextArrayWritable();	
			valueArray.set(valueText);
		
			context.write(userId,valueArray);
		}
		
		
	}
	
	public static class InfoFilterReducer extends Reducer<Text,TextArrayWritable,Text,Text>{
		
		@Override
		public void reduce(Text key,Iterable<TextArrayWritable> values,Context context) throws IOException, InterruptedException{
			String timeStamp = "0";
			String follow ="";
			String fans ="";
			for(TextArrayWritable value : values){
				Writable[] v = value.get();
				//compare the timeStamp to get the latest one
				if((v[2].toString().compareTo(timeStamp))>0){
					timeStamp = v[2].toString();
					follow = v[0].toString();
					fans = v[1].toString();
				}
			}
			context.write(key, new Text(follow +"|"+fans));
		}
		
	}

	
}
