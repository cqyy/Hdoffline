package com.wsc.hdbp.util;

import java.io.*;
import java.util.*;

public class SpecialWordBank {

	private static List<String> specialWords;

	static {
		specialWords = new ArrayList<String>();
		try {
			InputStream in = SpecialWordBank.class.getClassLoader()
					.getResourceAsStream("wordbank.properties");
			Properties properties = new Properties();
			properties.load(in);
			BufferedReader br = new BufferedReader(new InputStreamReader(
					SpecialWordBank.class.getClassLoader().getResourceAsStream(
							properties.getProperty("word_bank_file"))));
			String line;

			while ((line = br.readLine()) != null) {
				specialWords.add(line);
			}
			br.close();
			in.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static Map<String, Integer> getWordsOccuredInBank(
			Map<String, Integer> wordFrequency) {
		Map<String, Integer> wordFrequencyNeeded = new HashMap<String, Integer>();
		for (String specialWord : specialWords) {
			if (wordFrequency.containsKey(specialWord.toLowerCase())) {
				wordFrequencyNeeded.put(specialWord.toLowerCase(),
						wordFrequency.get(specialWord));
			}
		}
		return wordFrequencyNeeded;
	}
}
