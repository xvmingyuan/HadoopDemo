package analyze;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
//  uninet   mobile
public class Filter extends Mapper<LongWritable, Text, Text, NullWritable>{
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] fields = StringUtils.split(line);
			context.write(new Text(line), NullWritable.get());
//			if(fields.length>3 && StringUtils.isNotEmpty(fields[2])) {
//				
//			}else {
//				
//			}
			
		}
}
