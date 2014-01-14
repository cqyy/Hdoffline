package com.wsc.hdbp.offline.weibo;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


/**
 * <p>used to make an maker on files which be dealt</p>
 * */
public class FileMarker_deprecated {
	
	/**
	 * <p>make an maker on each matched files of {@code dir}</p>
	 * <p>the maker is generated from System.currentTimeMillis()</p>
	 * @param dir 
	 * 			the files path ,it could be one specific file ,or a regular expression.
	 * @param uri
	 * 			the URI to HDFS
	 * @return String-new files path,it points to files which be marked a maker just the step
	 * @author YuanYe
	 * */
	public static String makeMaker(String dir,String uri) throws IOException, URISyntaxException{
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI(uri),conf);
		String simpleDir = dir.substring(0, dir.lastIndexOf("/") + 1);
		String nameRegex = dir.substring(dir.lastIndexOf("/") + 1,dir.length());
		FileStatus[] fileStatus = fs.globStatus(new Path(dir));
		String timeStamp = System.currentTimeMillis()+"-";
		for (FileStatus f : fileStatus) {
			if (!f.isDir()) {
				Path tempPath = f.getPath();
				fs.rename(tempPath, new Path(simpleDir+timeStamp+tempPath.getName()));
			}
		}
		return simpleDir+timeStamp+nameRegex;
	}
}
