package com.wsc.hdbp.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsNamer {
	public static void main(String[] args) throws IOException, URISyntaxException{
		if(args.length != 2){
			System.err.println("input:<base diretory><name filter regex>");
			return;
		}
		
		Configuration conf = new Configuration();
		Properties properties = new Properties();
		properties.loadFromXML(
				new FileInputStream(
						new File("./conf/weibo.io.dir.xml")));
		
		//FileSystem fs = FileSystem.get(new URI(properties.getProperty("hdfs.uri")),conf);
		FileSystem fs = FileSystem.get(new URI("hdfs://192.168.3.130:9000"),conf);
		String baseDir = args[0];
		if(!baseDir.endsWith("/")){
			baseDir += "/";
		}
		Path dir = new Path(baseDir + args[1]);
		FileStatus 	filestatus[] = fs.globStatus(dir);
		
		for(FileStatus f : filestatus){
			if(!f.isDir()){
				String name = f.getPath().getName();
				name = name.substring(name.indexOf('-') + 1);
				fs.rename(f.getPath(), new Path(baseDir + name));
			}
		}
		
		
		
	}
}
