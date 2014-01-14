package com.wsc.hdbp.offline.weibo;

enum  RedisSelection {
	USER_INFO(3),
	WEIBO_CONTENT(4),
	WEIBO_REPLIES_REPEAT(6),
	FAN_FOLLOW(2),
	FAN_SHIP(1),
	REPEAT_REPLY_COUNT(11);
	
	
	private int index;
	
	RedisSelection(int index){
		this.index = index;
	};
	
	public int index(){
		return this.index;
	}
	
}
