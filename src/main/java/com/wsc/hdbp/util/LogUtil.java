package com.wsc.hdbp.util;

import org.apache.log4j.FileAppender;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

public class LogUtil {
	private static final Logger logger = Logger.getLogger(LogUtil.class);

	public static Logger getLogger() {
		return logger;
	}

	private static void setLog4jFile(Logger logger, String log_file) {
		FileAppender fappender = new FileAppender();
		PatternLayout layout = new PatternLayout();
		layout.setConversionPattern("%d{yyyy-MM-dd HH:mm:ss,SSS} %-5p (%F:%M(%L)) - %m%n");
		layout.activateOptions();
		fappender.setFile(log_file);
		fappender.setLayout(layout);
		fappender.activateOptions();
		logger.removeAllAppenders();
		logger.addAppender(fappender);
	}

	public static void init(String log_file) {
		setLog4jFile(logger, log_file);
	}

}
