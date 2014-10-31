package org.apache.hadoop.examples.yao;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * 倒排索引的程序
 * @author yaokj
 *
 */
public class InvertedIndex {

	// <pianyi,linecontent>-----> <word+filename,count>
	public static class InvertedIndexMapper extends Mapper<Object, Text, Text, Text> {
		private Text keyInfo = new Text();
		private Text valueInfo = new Text();
		private FileSplit fs = null;

		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			fs = (FileSplit) context.getInputSplit();
			String fileName = fs.getPath().getName();

			StringTokenizer tokenizer = new StringTokenizer(value.toString());
			while (tokenizer.hasMoreTokens()) {
				keyInfo.set(tokenizer.nextToken() + ":" + fileName);
				valueInfo.set("1");
				context.write(keyInfo, valueInfo);
			}
		}
	}

	// <word+filename,list(count)>-------> <word,filename+result>
	// result=the sum of list(count)
	public static class InvertedIndexCombiner extends Reducer<Text, Text, Text, Text> {
		private Text info = new Text();

		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int splitIndex = key.toString().indexOf(":");
			int sum = 0;
			String word = key.toString().substring(0, splitIndex);
			String fileName = key.toString().substring(splitIndex + 1);
			for (Text value : values) {
				sum += Integer.parseInt(value.toString());
			}
			info.set(fileName + ":" + sum);
			context.write(new Text(word), info);
		}
	}

	// <word,filename+result>------> <word,list(filename+result)>
	public static class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {

		private Text result = new Text();

		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String sum = new String();
			for (Text value : values) {
				sum += value.toString() + ";";
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		conf.addResource(new Path("/home/yaokj/hadoop-0.20.203.0/conf/hdfs-site.xml"));//配置文件上的位置
		conf.addResource(new Path("/home/yaokj/hadoop-0.20.203.0/conf/core-site.xml"));
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage : invertedindex ");
			System.exit(2);
		}
		Job job = new Job(conf, "invertedindex");
		job.setJarByClass(InvertedIndex.class);

		job.setMapperClass(InvertedIndexMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setCombinerClass(InvertedIndexCombiner.class);
		job.setReducerClass(InvertedIndexReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
