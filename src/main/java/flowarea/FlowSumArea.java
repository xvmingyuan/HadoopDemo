package flowarea;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import flowsum.FlowBean;

/**
 * 基于电话号区域分组
 * 数据源 /flow/data
 * @author xmy
 * @time：2018年7月2日 下午12:02:52
 */
public class FlowSumArea {
	public static class FlowSumAreaMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] fields = StringUtils.split(line, "\t");
			String phoneNB = fields[1];
			long up_flow = Long.parseLong(fields[7]);
			long d_flow = Long.parseLong(fields[8]);
			context.write(new Text(phoneNB), new FlowBean(phoneNB, up_flow, d_flow));

		}

	}

	public static class FlowSumAreaReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
		@Override
		protected void reduce(Text key, Iterable<FlowBean> values, Context context)
				throws IOException, InterruptedException {
			long up_flowcount = 0;
			long d_flowcount = 0;
			for (FlowBean flowBean : values) {
				up_flowcount += flowBean.getUp_flow();
				d_flowcount += flowBean.getD_flow();
			}
			context.write(key, new FlowBean(key.toString(), up_flowcount, d_flowcount));

		}

	}

	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(FlowSumArea.class);

		job.setMapperClass(FlowSumAreaMapper.class);
		job.setReducerClass(FlowSumAreaReducer.class);
		// 设置自定义的分组逻辑
		job.setPartitionerClass(Areapartitioner.class);

		// job.setMapOutputKeyClass(Text.class);
		// job.setMapOutputValueClass(FlowBean.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);
		// 开启线程(与Areapartitioner分类个数保持一致,或线程数大于分类数,线程数少于分类数会报错,但是一个线程除外)
		job.setNumReduceTasks(6);

//		FileInputFormat.setInputPaths(job, new Path("hdfs://it01:9000/flow/data"));
//		FileOutputFormat.setOutputPath(job, new Path("hdfs://it01:9000/flow/area3"));
//		FileInputFormat.setInputPaths(job, new Path("hdfs://ns1/flow/data"));
//		FileOutputFormat.setOutputPath(job, new Path("hdfs://ns1/flow/area3"));
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
