package ii;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/**
 * 字段在指定文件出现频次计算 step one
 * @author xmy
 * @time：2018年7月13日 下午12:14:57
 */
public class InverseIndexStepOne {
	public static class StepOneMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// 获取一行数据,格式:hello tom
			String line = value.toString();
			// 根据空格切分数据
			String[] fields = StringUtils.split(line, " ");
			// 获取当前文件所在的路径对象
			FileSplit inputSplit = (FileSplit) context.getInputSplit();
			// 获取文件名称,格式:a.txt
			String filename = inputSplit.getPath().getName();
			// 遍历字段数组
			for (String field : fields) {
				// 获取当前字段,添加字段所在的文件名称,将字段计数为1,输出结果到context中,mapoutput格式:{k:hello-->a.txt,v:1}
				context.write(new Text(field + "-->" + filename), new LongWritable(1));
			}
		}
	}

	public static class StepOneReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
		@Override
		protected void reduce(Text key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {
			long count = 0;
			// {key:hello-->a.txt,values:[1,1,1,1,,1,1,1]}
			for (LongWritable num : values) {
				count += num.get();
			}
			context.write(key, new LongWritable(count));
		}
	}

	public static void main(String[] args) throws Exception {
		// 配置hadoop 资源文件 默认读取根目录下的core-site.xml 和hdfs-site.xml
		Configuration conf = new Configuration();
		
		// 创建一个集群 job工作 
		Job jobone = Job.getInstance(conf);
		// 配置 job 执行的InverseIndexStepOne.class文件的main方法
		jobone.setJarByClass(InverseIndexStepOne.class);
		
		// job 执行mapper 的.class为:StepOneMapper.class
		jobone.setMapperClass(StepOneMapper.class);
		// job 执行reducer 的.class为:StepOneReducer.class
		jobone.setReducerClass(StepOneReducer.class);
		
		// job 设置mapper和reducer的输出结果类型, 
		// 如果两者的输出类型一样,可以省略mapper的输出结构类型的设置(当前任务输出类型一致,故写一个)
		jobone.setOutputKeyClass(Text.class);
		jobone.setOutputValueClass(LongWritable.class);
		
		// job添加输入数据的路径
		FileInputFormat.setInputPaths(jobone, new Path(args[0]));
		// job的数据输出路径校验 
		Path output = new Path(args[1]);
		FileSystem fileSystem = FileSystem.get(conf);
		// 存在当前路径,删除当前存在路径
		if (fileSystem.exists(output)) {
			// 删除路径 true 代表递归删除
			fileSystem.delete(output, true);
		}
		// job添加处理结果的数据输出路径
		FileOutputFormat.setOutputPath(jobone, output);
		// job等待执行完成
		System.exit(jobone.waitForCompletion(true) ? 0 : 1);

	}

}
