package com.wsc.hdbp.offline.weibo;

/**
 * <p>the entrance of the whole programs</p>
 * 
 * @author YuanYe
 * */

public class Main {
	public static void main(String[] args) throws Exception{
		String usage = "Usage:input the index of work.\n"
						+ "1  :  WeiboContentJoin\n"
						+ "2  :  WeiboContentWordCount\n"
						+ "3  :  WeiboRepliesRepeatWordCount\n"
						+ "4  :  WeiboUserFans\n"
						+ "5  :  WeiboUserFilter\n"
						+ "6  :  WeiboUserFollow\n"
						+ "7  :  WeiboUserInfo\n"
		                + "8  :  CommentRepeatCount";

		if(args.length != 1){
			System.err.println("Arguments Not Match,Only One Argument");
			System.err.println(usage);
			System.exit(1);
		}
		
		int workType = 0;
		
		try{
			workType = Integer.valueOf(args[0]);
			if(workType>8 || workType <1) throw new Exception();
		}catch(Exception e){
					System.err.println("Invalid input");
					System.err.println(usage);
					System.exit(1);
			}
		
		int exitCode = 0;
		switch(workType){
		case 1:
			exitCode = new WeiboContentJoinDriver().run(null);
			break;
		case 2:
			exitCode = new WeiboContentWordCountDriver().run(null);
			break;
		case 3:
			exitCode = new WeiboRepliesRepeatWordCountDriver().run(null);
			break;
		case 4:
			exitCode = new WeiboUserFansDriver().run(null);
			break;
		case 5:
			exitCode = new WeiboUserFilterDriver().run(null);
			break;
		case 6:
			exitCode = new WeiboUserFollowDriver().run(null);
			break;
		case 7:
			exitCode = new WeiboUserInfoDriver().run(null);
			break;
		case 8:
			exitCode = new WeiboRepliesRepeatCountDriver().run(null);
			break;
		}
		System.exit(exitCode);
	}
}
