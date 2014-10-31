package org.apache.hadoop.examples.yao;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;

public class ReadLocalFile2Hadoop {

	public static void main(String[] args)  throws IOException{
		readLocalFile2Hadoop("/home/yaokj/temp","test");
	}
	
	/**
	 * 以流形式上传本地文件到分布式文件系统中
	 * @param inputDir 本地文件夹
	 * @param hdfsDir　　　Hadoop 上的文件夹
	 * @throws IOException
	 */
	public static void readLocalFile2Hadoop(String inputDir,String hdfsDir) throws IOException{
		Configuration cfg = new Configuration();
		cfg.addResource(new Path("/home/yaokj/hadoop-0.20.203.0/conf/hdfs-site.xml"));//配置文件上的位置
		cfg.addResource(new Path("/home/yaokj/hadoop-0.20.203.0/conf/core-site.xml"));
		FileSystem fs = FileSystem.get(cfg);
		LocalFileSystem localFS = FileSystem.getLocal(cfg);
		
		fs.mkdirs(new Path(hdfsDir));
		
		FileStatus[] inputFiles =  localFS.listStatus(new Path(inputDir));
		
		FSDataOutputStream out ;
		FSDataInputStream in;
		for (int i = 0 ; i < inputFiles.length ; i++) {
			System.out.println(inputFiles[i].getPath().getName());
			
			in = localFS.open(inputFiles[i].getPath());
			out = fs.create(new Path(hdfsDir+inputFiles[i].getPath().getName()));
			
			byte[]  buffer = new byte[256];
			
			int byteRead = 0 ;
			while ((byteRead = in.read(buffer)) > 0) {
				out.write(buffer, 0, byteRead);
			}
			
			out.close();
			in.close();
			
			File file = new File(inputFiles[i].getPath().toString());
			//System.out.println(inputFiles[i].getPath().toString());
			System.out.println(file.delete());
		}
		
	}
	
}
