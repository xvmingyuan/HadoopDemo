package flowsort;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.commons.lang.StringUtils;

import flowsum.FlowBean;
/**
 * 基于流量统计后文件,降序排列流量
 * 数据源 /flow/sum
 * @author xmy
 * @time：2018年7月2日 下午12:01:23
 */
public class SortMR {
	
	public static class SortMapper extends Mapper<LongWritable, Text, FlowBean, NullWritable> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] fields = StringUtils.split(line, "\t");
			String phoneNB = fields[0];
			long u_flow = Long.parseLong(fields[1]);
			long d_flow = Long.parseLong(fields[2]);
			context.write(new FlowBean(phoneNB, u_flow, d_flow), NullWritable.get());

		}
	}
	public static class SortReducer extends Reducer<FlowBean, NullWritable, Text, FlowBean>{
		@Override
		protected void reduce(FlowBean key, Iterable<NullWritable> value,Context context)
				throws IOException, InterruptedException {
			context.write(new Text(key.getPhoneNB()), key);
		}
	}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(SortMR.class);

		job.setMapperClass(SortMapper.class);
		job.setReducerClass(SortReducer.class);

		job.setMapOutputKeyClass(FlowBean.class);
		job.setMapOutputValueClass(NullWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true)?0:1 );
	}
}
