package com.wsc.hdbp.offline.weibo;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


/**
 * make an maker on files,maker is a time stamp of current time mill
 * @author YuanYe
 * */
public class FileMarker {
	
	
	///avoid create an instance of this class
	private FileMarker(){};
	
	/**
	 * <p>rename the files in the given path,rename strategy is add a time stamp to the head of file</p>
	 * <p>this function just point to one path ,and it won't deal files in the sub paths</p>
	 * @param inpath
	 * 			the path in which files to be added an marker 
	 * @param uri
	 * 			HDFS URI
	 * @author YuanYe
	 * */
	public static void makeMarker(Path inpath,String uri) throws IOException, URISyntaxException{
		String basedir = inpath.toString();
		basedir = basedir.substring(0,basedir.lastIndexOf('/')+1);
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI(uri), conf);
		FileStatus[] fsts = fs.globStatus(inpath);
		long timestamp = System.currentTimeMillis();
		
		for(FileStatus f : fsts){
			if(!f.isDir()){
				String name = basedir + timestamp + "-" + f.getPath().getName();
				fs.rename(f.getPath(),new Path(name));
			}
		}
		fs.close();
	}
}
