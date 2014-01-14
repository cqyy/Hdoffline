package com.wsc.hdbp.offline.weibo;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/*generate an path directory which does't exits to be used as an temporary directory*/
public class TempPathHandler {
	/**
	 * get an path does't exist
	 * @param basePath
	 * @param uri
	 * @author YuanYe
	 * */
	public static String tempPath(String basePath,String uri) throws IOException, URISyntaxException{
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI(uri),conf);
		if(!fs.exists(new Path(basePath))){
			return basePath;
		}
		String path = basePath.endsWith("/")
				?basePath.substring(0,basePath.length()-1)
						:basePath;
		int count = 1;
		while(true){
			String tempPath = path+count+"/";
			if(!fs.exists(new Path(tempPath)))
			return tempPath;
		}
	} 

}
