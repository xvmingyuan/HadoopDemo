package analyze;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LogEnhanceOutputFormat<K, V> extends FileOutputFormat<K, V> {

	@Override
	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
		FileSystem fs = FileSystem.get(new Configuration());
		return new LogEnhanceRecordWriter<K, V>(fs.create(new Path("/ANA/output/uninet")),
				fs.create(new Path("/ANA/output/mobile")));
	}

	public static class LogEnhanceRecordWriter<K, V> extends RecordWriter<K, V> {
		private FSDataOutputStream uninet = null;
		private FSDataOutputStream mobile = null;

		public LogEnhanceRecordWriter(FSDataOutputStream uninet, FSDataOutputStream mobile) {
			this.mobile = mobile;
			this.uninet = uninet;
		}

		@Override
		public void write(K key, V value) throws IOException, InterruptedException {
			String line = key.toString();
			String[] fields = StringUtils.split(line);
			if(fields.length>3 && StringUtils.isNotEmpty(fields[2]) && fields[2].toString().contains(".")) {
				//有线
				uninet.write((key.toString()+"\r\n").getBytes());
			}else {
				//无线
				mobile.write((key.toString()+"\r\n").getBytes());
				
			}

		}

		@Override
		public void close(TaskAttemptContext arg0) throws IOException, InterruptedException {
			if(mobile!=null)
				mobile.close();
			if(uninet!=null)
				uninet.close();

		}

	}

}
