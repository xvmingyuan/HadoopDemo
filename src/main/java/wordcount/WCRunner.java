package wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WCRunner {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		Job wcjob = Job.getInstance(conf);
		// 1.指定WC的路径(默认寻找WCRunner当前目录)
		wcjob.setJarByClass(WCRunner.class);
		
		// 2.指定本job使用的Mapper和Reducer的类
		wcjob.setMapperClass(WCMapper.class);
		wcjob.setReducerClass(WCReducer.class);

		// 3.map 输出类型
		wcjob.setMapOutputKeyClass(Text.class);
		wcjob.setMapOutputValueClass(LongWritable.class);

		// 4.reduce 输出类型
		wcjob.setOutputKeyClass(Text.class);
		wcjob.setOutputValueClass(LongWritable.class);

		// 5.指定处理的输入数据存放路径
//		FileInputFormat.setInputPaths(wcjob, new Path("/wc/srcdata/")); //集群模式(打包jar 提交给集群运行)
		FileInputFormat.setInputPaths(wcjob, new Path("hdfs://ns1/wc/srcdata/")); //本地运行调用服务器文件
//		FileInputFormat.setInputPaths(wcjob, new Path("/Users/xmy/Desktop/wc/srcdata")); //纯本地模式
		
		// 6.处理的输出数据存放路径 
//		FileOutputFormat.setOutputPath(wcjob, new Path("/wc/output/")); //集群模式(打包jar 提交给集群运行)
		FileOutputFormat.setOutputPath(wcjob, new Path("hdfs://ns1/wc/output/")); //本地运行输出到服务器文件
//		FileOutputFormat.setOutputPath(wcjob, new Path("/Users/xmy/Desktop/wc/output/")); //纯本地模式
		
		// 7.the job submit to the hadoop to run
		wcjob.waitForCompletion(true);
		
	}

}
