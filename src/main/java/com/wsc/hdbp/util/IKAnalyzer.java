package com.wsc.hdbp.util;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.*;
import java.util.HashMap;

public class IKAnalyzer {
	public static void main(String[] args) throws Exception {
		String str = "本来上周就可以上架的,结果小凡突然跑去三峡旅游去了,嘿嘿。。每幅画都有它的名字《仙客来》、《窗户》、《橘子》、《蔷薇窗》、《青青小村》、停泊的时光》、《温柔》、《AGEDBEAUTY》,你知道哪个名字对应的是哪幅画吗？【索肥丫.sofia绘画作品】无框画（30cm&times;45cm）";
		System.out.println(weiboWord(str));
		System.out.println(getWordFrequency(str));
	}

	public static HashMap<String, Integer> IKAnalysis(String str) {
		HashMap<String, Integer> res = new HashMap<String, Integer>();
		try {
			// InputStream in = new FileInputStream(str);//
			byte[] bt = str.getBytes();// str
			InputStream ip = new ByteArrayInputStream(bt);
			Reader read = new InputStreamReader(ip);
			IKSegmenter iks = new IKSegmenter(read, true);
			Lexeme t;
			String temp;
			while ((t = iks.next()) != null) {
				temp = t.getLexemeText();
				if (res.containsKey(temp)) {
					res.put(temp, res.get(temp) + 1);
				} else {
					res.put(temp, 1);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return res;
	}

	public static HashMap<String, Integer> IKAnalysis(String str, float[] count) {
		HashMap<String, Integer> res = new HashMap<String, Integer>();
		try {
			// InputStream in = new FileInputStream(str);//
			byte[] bt = str.getBytes();// str
			InputStream ip = new ByteArrayInputStream(bt);
			Reader read = new InputStreamReader(ip);
			IKSegmenter iks = new IKSegmenter(read, true);
			Lexeme t;
			String temp;
			while ((t = iks.next()) != null) {
				temp = t.getLexemeText();
				if (res.containsKey(temp)) {
					res.put(temp, res.get(temp) + 1);
				} else {
					res.put(temp, 1);
				}
				count[0] += 1.0;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return res;
	}

	public static HashMap<String, Integer> getWordFrequency(String str) {
		HashMap<String, Integer> res = new HashMap<String, Integer>();
		try {
			// InputStream in = new FileInputStream(str);//
			byte[] bt = str.getBytes();// str
			InputStream ip = new ByteArrayInputStream(bt);
			Reader read = new InputStreamReader(ip);
			IKSegmenter iks = new IKSegmenter(read, true);
			Lexeme t;
			String temp;
			while ((t = iks.next()) != null) {
				temp = t.getLexemeText();
				if (temp.length() > 1) {// 分词长度必须大于1
					// TODO 另外如果中英混合 中数混合 英数混合 则忽略
					if (res.containsKey(temp)) {
						res.put(temp, res.get(temp) + 1);
					} else {
						res.put(temp, 1);
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return res;
	}

	public static HashMap<String, Float> weiboWord(String text) {
		HashMap<String, Float> wf = new HashMap<String, Float>();
		float[] count = new float[1];
		HashMap<String, Integer> wc = IKAnalysis(text, count);
		for (String key : wc.keySet()) {
			wf.put(key, wc.get(key) / count[0]);
		}
		return wf;
	}
}
