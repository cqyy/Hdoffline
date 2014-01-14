package com.wsc.hdbp.offline.renren;

import com.wsc.hdbp.offline.outputformat.RedisOutputFormatForHash;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * 文件来源info.txt.* 将人人网个人信息从hdfs导入Redis
 * 
 * @author Elliot
 */
public class RenInfo {

	public final static class TokenizerMapper extends
			Mapper<LongWritable, Text, Text, MapWritable> {

		Text outKey = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String oneInfo = line.substring(0, line.length() - 3);
			String[] col = oneInfo.split(",");// 英文逗号
			if (col.length != 28) {// 规定28字段
				return;
			}
			for (int i = 0; i <= 27; i++) {// 字段首尾空格
				col[i] = col[i].trim();
			}
			String userId = col[0];// 用户ID
			String name = col[1];// 用户名
			String level = col[2];// 用户等级
			String avatar = col[3];// 用户头像
			String gender = col[4];// 性别
			String birthdayInfo = col[5];// 生日+星座
			String hometown = col[6];// 家乡
			String numConnections = col[7];// 好友数
			String blogNum = col[8];// 日志数
			String shareNum = col[9];// 分享数
			String statusNum = col[10];// 状态数
			String collegeInfo = col[11];// 大学
			String highSchoolInfo = col[12];// 高中
			String middleSchoolInfo = col[13];// 初中
			String primarySchoolInfo = col[14];// 小学
			String company = col[15];// 工作单位
			String workDate = col[16];// 工作时间
			String phoneNum = col[17];// 手机号
			String qq = col[18];// qq
			String msn = col[19];// msn
			String personalSite = col[20];// 个人网站
			String interests = col[21];// 兴趣爱好-以下都是*号分割
			String lovedMusic = col[22];// 音乐
			String lovedMovie = col[23];// 电影
			String lovedGame = col[24];// 游戏
			String lovedCommic = col[25];// 动漫
			String lovedSport = col[26];// 运动
			String lovedBook = col[27];// 书

			outKey.set(userId);
			MapWritable map = new MapWritable();
			map.put(new Text("nick"), new Text(name));
			map.put(new Text("gender"), new Text(gender));
			map.put(new Text("numConnections"), new Text(numConnections));
			map.put(new Text("blogCount"), new Text(blogNum));
			map.put(new Text("shareCount"), new Text(shareNum));
			map.put(new Text("statusCount"), new Text(statusNum));
			map.put(new Text("avatar"), new Text(avatar));
			map.put(new Text("hometown"), new Text(hometown));
			map.put(new Text("qq"), new Text(qq));
			map.put(new Text("level"), new Text(level));
			map.put(new Text("msn"), new Text(msn));
			map.put(new Text("tel"), new Text(phoneNum));
			map.put(new Text("personalSite"), new Text(personalSite));
			map.put(new Text("birthdayInfo"), new Text(birthdayInfo));
			map.put(new Text("collegeInfo"), new Text(collegeInfo));
			map.put(new Text("highSchoolInfo"), new Text(highSchoolInfo));
			map.put(new Text("middleSchoolInfo"), new Text(middleSchoolInfo));
			map.put(new Text("primarySchoolInfo"), new Text(primarySchoolInfo));
			map.put(new Text("company"), new Text(company));
			map.put(new Text("workDate"), new Text(workDate));
			map.put(new Text("interests"), new Text(interests));
			map.put(new Text("lovedMusic"), new Text(lovedMusic));
			map.put(new Text("lovedMovie"), new Text(lovedMovie));
			map.put(new Text("lovedGame"), new Text(lovedGame));
			map.put(new Text("lovedCommic"), new Text(lovedCommic));
			map.put(new Text("lovedSport"), new Text(lovedSport));
			map.put(new Text("lovedBook"), new Text(lovedBook));

			context.write(outKey, map);
		}
	}

	public static void main(String[] args) {
		InputStream in = RenInfo.class.getClassLoader().getResourceAsStream(
				"redis.properties");
		Properties properties = new Properties();
		try {
			properties.load(in);
		} catch (IOException exception) {
			exception.printStackTrace();
		}
		// 读取配置文件中的redis服务器IP
		String redisHost = properties.getProperty("redis.server.host");
		Path inputPath = new Path(args[0]);

		try {
			Job buildFollowCounts = new Job(new Configuration(),
					"Build Renren UserInfo In Redis");
			buildFollowCounts.setMapperClass(TokenizerMapper.class);
			buildFollowCounts.setJarByClass(RenInfo.class);
			buildFollowCounts.setMapOutputKeyClass(Text.class);
			buildFollowCounts.setMapOutputValueClass(MapWritable.class);

			buildFollowCounts.setInputFormatClass(TextInputFormat.class);
			buildFollowCounts
					.setOutputFormatClass(RedisOutputFormatForHash.class);
			RedisOutputFormatForHash
					.setRedisHosts(buildFollowCounts, redisHost);
			RedisOutputFormatForHash.setRedisSelection(buildFollowCounts, 5);// 人人网个人信息存储数据库5

			buildFollowCounts.setOutputKeyClass(Text.class);
			buildFollowCounts.setOutputValueClass(Text.class);

			TextInputFormat.addInputPath(buildFollowCounts, inputPath);

			System.exit(buildFollowCounts.waitForCompletion(false) ? 0 : 1);

		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

}
