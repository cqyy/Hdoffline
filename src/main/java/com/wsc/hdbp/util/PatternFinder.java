package com.wsc.hdbp.util;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PatternFinder {

	private static Pattern p;
	private static Matcher m;

	/**
	 * 找到被提到的人,即用户原创内容中的@对象.主要有点：1,单个字符不能算词
	 * 
	 * @param src
	 *            被匹配的文本.
	 * @return
	 */
	public static List<String> findMention(String src) {

		src = src.split("//@", 2)[0];
		List<String> resList = new ArrayList<String>();
		// String regex = "(?<=@)\\S+?(?=[\\s:])";
		String regex = "(?<=@)\\S+?(?=[^\u4e00-\u9fa50-9a-zA-Z_-])";
		p = Pattern.compile(regex);
		m = p.matcher(src);

		while (m.find()) {
			String res = m.group();
			resList.add(res);
		}
		return resList;
	}

	/**
	 * 找到之前多个被转发者,标志是"//@".
	 * 
	 * @param src
	 *            被匹配的文本.
	 * @return
	 */
	public static List<String> findTweetee(String src) {

		List<String> resList = new ArrayList<String>();
		String regex = "(?<=//@)\\S+?(?=[^\u4e00-\u9fa50-9a-zA-Z_-])";
		p = Pattern.compile(regex);
		m = p.matcher(src);

		while (m.find()) {
			resList.add(m.group());
		}
		return resList;
	}

	/**
	 * 找到上一个被转发者,标志是"//@".
	 * 
	 * @param src
	 *            被匹配的文本.
	 * @return
	 */
	public static String findTweetee2(String src) {

		String regex = "(?<=//@)\\S+?(?=[^\u4e00-\u9fa50-9a-zA-Z_-])";
		p = Pattern.compile(regex);
		m = p.matcher(src);
		String res = null;
		if (m.find()) {
			res = m.group();
		}
		return res;
	}

	/**
	 * 去处转发标记(一般为开头到//@)
	 * 我说的话//@某人:....//@某人:....
	 * @param weibostr
	 *            微博原始字串
	 */
	public static String cleanSymbolOfRetweet(String weibostr) {
		String regex = "^.*?(?=//@)";
		p = Pattern.compile(regex);
		m = p.matcher(weibostr);
		String res = weibostr;
		if (m.find()) {
			res = m.group();
		}
		return res;
	}

	/**
	 * 在微博字符串中找出作者本人所发的内容
	 * 我说的话@某人 。。。。
	 * @param weibostr
	 *            微博原始字串
	 */
	public static String cleanSymbolOfMention(String weibostr) {
		String regex1 = "@[a-zA-Z0-9\u4E00-\u9FA5-_]*?(?=[^a-zA-Z0-9\u4E00-\u9FA5-_])";
		String regex2 = "@[a-zA-Z0-9\u4E00-\u9FA5-_]*?$";
		String res = weibostr;
		try {
			res = res.replaceAll(regex1, "");
		} catch (java.lang.NullPointerException e) {
		}
		try {
			res = res.replaceAll(regex2, "");
		} catch (java.lang.NullPointerException e) {
		}
		return res;
	}

	/**
	 * 去处字符串中的t.cn网址
	 * 
	 * @param str
	 *            原始字符串
	 * @return 去除网址之后的字符串
	 */
	public static String cleanSymbolOfWeiboUrls(String str) {
		String regex = "http://t.cn/[0-9a-zA-Z]*";
		// String regex = "http://([\\w-]+\\.)+[\\w-]+(/[\\w- ./?%&=]*)?";
		String res = str;
		try {
			res = res.replaceAll(regex, "");
		} catch (java.lang.NullPointerException e) {
		}
		return res;
	}

	/**
	 * 拿到一个评论列表，返回所有的评论评论内容
	 * 
	 * @param str
	 *            爬虫得到的某微博的评论列表
	 * @return List 评论内容
	 */
	public static List<String> getCommentList(String commentStr) {
		List<String> commentsList = new ArrayList<String>();
		String regex = "(?<=：).*?(?=\\([(\\d+?月)今])";
		p = Pattern.compile(regex);
		m = p.matcher(commentStr);
		while (m.find()) {
			commentsList.add(m.group());
		}
		return commentsList;
	}

	/**
	 * 拿到一个评论列表的长度
	 * 
	 * @param str
	 *            爬虫得到的某微博的评论列表
	 * @return int 评论列表长度（评论数）
	 */
	public static int getCommentCount(String commentStr) {
		int result = 0;
		String regex = "(?<=：).*?(?=\\([(\\d+?月)今])";
		p = Pattern.compile(regex);
		m = p.matcher(commentStr);
		while (m.find()) {
			result += 1;
		}
		return result;
	}

}
