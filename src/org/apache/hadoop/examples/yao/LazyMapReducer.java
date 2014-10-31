package org.apache.hadoop.examples.yao;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class LazyMapReducer {
	/**
	 * 查看Map Reduce 默默做了一些什么
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.addResource(new Path("/home/yaokj/hadoop-0.20.203.0/conf/hdfs-site.xml"));//配置文件上的位置
		conf.addResource(new Path("/home/yaokj/hadoop-0.20.203.0/conf/core-site.xml"));
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "LazyMapReducer");
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
