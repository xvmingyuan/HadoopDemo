package wordcount;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WCReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
	// 框架在map处理完成后,将所有kv对缓存起来,进行分组,然后传递一组<key,values{}>,调用一次reduce方法
	// 例如:<hello,{1,1,1,1,1...}> key=hello  values=[1,1,1,1...]
	@Override
	protected void reduce(Text key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {
		long count = 0;
		// 遍历value的list,进行累加求和
		for (LongWritable value : values) {
			count += value.get();

		}
		// 输出这一个单词的统计结果
		context.write(key, new LongWritable(count));
	}

}
