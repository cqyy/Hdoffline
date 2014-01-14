package com.wsc.hdbp.offline.weibo;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class WeiboRepliesRepeatCount {

    public static class RRCountMapper extends Mapper<Object,Text,Text,MapWritable>{
	
		@Override
		public void map(Object obj,Text value, Context context)
                throws IOException, InterruptedException {

			String[] delem = value.toString().split("\t",2);
			if (delem.length != 2) {
				return;
			}
			
			int nComment = 0;
			int nRepeat = 0;
			String userId = delem[0].trim();
			String content = delem[1];
			int splitIndex1 = content.indexOf("|");
			int splitIndex2 = content.lastIndexOf("|");

			String comment = content.substring(splitIndex1+1,splitIndex2);
			String repeat = content.substring(splitIndex2+1);
			
			//comment
			if(comment.trim().length() > 0){
				nComment++ ;
			}
			
			//repeat
			if(repeat.trim().length() > 0){
				nRepeat++;
			}
			
			MapWritable out = new MapWritable();
			out.put(new Text("c"), new IntWritable(nComment));
			out.put(new Text("r"), new IntWritable(nRepeat));
			
			context.write(new Text(userId), out);
		}
	}
	
	public static class RRReducer extends Reducer<Text,MapWritable,Text,Text>{
		
		@Override
		public void reduce(Text key, Iterable<MapWritable> values,Context context)
                throws IOException, InterruptedException{
			int nComment = 0;
			int nRepeat = 0;
			Text c = new Text("c");
            Text r = new Text("r");

			for(MapWritable map : values){
				nComment += Integer.valueOf(map.get(c).toString());
				nRepeat += Integer.valueOf(map.get(r).toString());
			}
			
			String value = nComment + "|" + nRepeat;
			context.write(key, new Text(value));
		}
	}

    //second phase , get result ,sent to Redis
    public static class RRCountOutMapper extends Mapper<Object,Text,Text,MapWritable>{

        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] split = value.toString().split("\t",2);
            if(split.length != 2){
                return;
            }
            String userId = split[0];
            String[] count = split[1].split("\\|");
            if(count.length != 2){
                return;
            }

            int comment = Integer.valueOf(count[0]);
            int repeat = Integer.valueOf(count[1]);

            MapWritable map = new MapWritable();
            map.put(new Text("c"),new IntWritable(comment));
            map.put(new Text("r"),new IntWritable(repeat));

            context.write(new Text(userId),map);
        }
    }
}
