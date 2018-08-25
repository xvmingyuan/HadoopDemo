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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/**
 * 字段在指定文件出现频次计算 step two
 * @author xmy
 * @time：2018年7月13日 下午12:14:57
 */
public class InverseIndexStepTwo {
	// hello-->a.txt 5
	// hello-->b.txt 4
	// hello-->c.txt 3
	public static class StepTwoMapper extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
//			input hello-->c.txt 3
			String line = value.toString();
			String[] fields = StringUtils.split(line,"\t");
			String[] wordAndfileName = StringUtils.split(fields[0],"-->");
			String word = wordAndfileName[0];
			String fileName = wordAndfileName[1];
			long count = Long.parseLong(fields[1]);
			// mapout:<hello, a.txt-->3
			context.write(new Text(word), new Text(fileName+"-->"+count));
		}

	}
	public static class StepTwoReducer extends Reducer<Text, Text, Text, Text>{
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// mapinput:<hello, {a.txt-->3,b.txt--->2,c.txt-->1}>
			StringBuffer string = new StringBuffer();
			for (Text text : values) {
				string.append(text+" ");
			}
			context.write(new Text(key),new Text(string.toString()));
			
			
		}
	}
public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		Job jobtwo = Job.getInstance(conf);
		jobtwo.setJarByClass(InverseIndexStepTwo.class);
		
		jobtwo.setMapperClass(StepTwoMapper.class);
		jobtwo.setReducerClass(StepTwoReducer.class);
		
		jobtwo.setOutputKeyClass(Text.class);
		jobtwo.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(jobtwo, new Path(args[0]));
		
		Path output = new Path(args[1]);
		FileSystem fileSystem = FileSystem.get(conf);
		if (fileSystem.exists(output)) {
			fileSystem.delete(output, true);
		}
		FileOutputFormat.setOutputPath(jobtwo, output);
		
		System.exit(jobtwo.waitForCompletion(true) ? 0 : 1);

	}

}
