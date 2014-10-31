package org.apache.hadoop.examples.yao;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;

public class FileDemo {

	public static void main(String[] args) throws IOException {
		/*mkdir("test");
		putFile("/home/yaokj/temp.txt", "test/test.txt");
		createFile("test/create.txt");
		rename("test/test.txt", "test/test2.txt");
		System.out.println(deleteFile("test/test2.txt"));
		System.out.println(getLastModifyTime("test/create.txt"));
		System.out.println(isExists("test/create.txt"));
		System.out.println(isExists("test/test2.txt"));*/
		
		printDataNodeStatus();
	}

	public static void printDataNodeStatus() throws IOException {
		FileSystem fs = getFileSystem();
		DistributedFileSystem dfs = (DistributedFileSystem) fs;
		DatanodeInfo[] dataInfos = dfs.getDataNodeStats();
		if (dataInfos != null && dataInfos.length > 0) {
			for (int i = 0; i < dataInfos.length; i++) {
				System.out.println("host: " + dataInfos[i].getHostName());
				System.out.println("host: " + dataInfos[i].getHost());
			}

		}
	}

	public static boolean isExists(String path) throws IOException {
		FileSystem fs = getFileSystem();
		return fs.exists(new Path(path));
	}

	public static long getLastModifyTime(String path) throws IOException {
		FileSystem fs = getFileSystem();
		FileStatus file = fs.getFileStatus(new Path(path));
		return file.getModificationTime();
	}

	public static boolean deleteFile(String path) throws IOException {
		FileSystem fs = getFileSystem();
		return fs.delete(new Path(path), false);// false 为是否递归删除
	}

	public static void rename(String fromFile, String toFile) throws IOException {
		FileSystem fs = getFileSystem();
		fs.rename(new Path(fromFile), new Path(toFile));
		fs.close();
	}

	public static void createFile(String file) throws IOException {
		FileSystem fs = getFileSystem();
		FSDataOutputStream fsd = fs.create(new Path(file));
		byte[] witeByte = "Hello world , you know".getBytes();
		fsd.write(witeByte, 0, witeByte.length);

		fsd.close();
		fs.close();
	}

	public static void putFile(String srcPath, String dstPath) throws IOException {
		FileSystem fs = getFileSystem();

		Path src = new Path(srcPath);
		Path dst = new Path(dstPath);

		fs.copyFromLocalFile(src, dst);

		FileStatus[] fileStatus = fs.listStatus(dst);

		for (FileStatus status : fileStatus) {
			System.out.println(status.getPath());
		}

		fs.close();
	}

	public static void mkdir(String dst) throws IOException {
		if (dst != null && !"".equals(dst)) {
			FileSystem fs = getFileSystem();
			fs.mkdirs(new Path(dst));
			fs.close();
		}

	}

	public static FileSystem getFileSystem() throws IOException {
		Configuration cfg = new Configuration();
		cfg.addResource(new Path("/home/yaokj/hadoop-0.20.203.0/conf/hdfs-site.xml"));
		cfg.addResource(new Path("/home/yaokj/hadoop-0.20.203.0/conf/core-site.xml"));
		FileSystem fs = FileSystem.get(cfg);
		return fs;
	}

}
