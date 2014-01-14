package com.wsc.hdbp.offline.weibo;
/**
 * get content and replies of the weibo from the String
 * the weibo content contain the content as well as replies
 * and it contain @person and so on 
 * so this class is aimed to deal this situation get the pure content and replies
 * 
 * @author YuanYe
 * @date 2013-11-28*/
public class WeiboSplitter {
	
	/**
	 * @function get the content of the weibo ;the content doesn't contain the @person,the URL
	 * this function is done by regular expression.
	 * @param originalContent
	 * the original weibo content
	 * */
	public static String pureContent(String originalContent){
		String result = clearSymbolOfRepeat(originalContent);
		result = clearURL(result);
		result = clearAtPerson(result);
		return result;
	}
	
	/**
	 * @function clear the symbol of repeat
	 * the repeat symbol is in the pattern like //@personName:original content
	 * @param content
	 * the weibo content
	 * */
	public static String clearSymbolOfRepeat(String content){
		String symbol="//@";
		String[] results = content.split(symbol);
		return (results.length == 0)?"":results[0];
	}
	
	/**
	 * @function clear the URL in the content
	 * @param content
	 * the WeiBo content*/
	public static String clearURL(String content){
		/**
		 * regular expression;
		 * this expression is specific to the URL of the weibo
		 * */
		String regex="http://t.cn/[0-9a-zA-Z]*";
		return content.replaceAll(regex, "");
	}
	
	/**
	 * @function clear the at person
	 * sometimes the weibo contain some at person,in the pattern @personName
	 * @param content
	 * the weibo content*/
	public static String clearAtPerson(String content){
		/*regular expression
		 * the @person is begin with the symbol '@' 
		 * and end with a English blank
		 * */
		String regex="@.* ";
		return content.replaceAll(regex, "");
	}
}
