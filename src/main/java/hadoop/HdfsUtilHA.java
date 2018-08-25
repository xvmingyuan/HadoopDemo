package hadoop;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

public class HdfsUtilHA {
	FileSystem fs = null;
	
	@Before
	public void init() throws Exception {
		Configuration conf = new Configuration();
		fs = FileSystem.get(new URI("hdfs://ns1/"), conf, "xmy");
	}

	// 底层写法(上传)
	public static void main1(String[] args) throws IOException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://ns1/");
		FileSystem fs = FileSystem.get(conf);
		Path dts = new Path("hdfs://ns1/aa/data.txt");
		FSDataOutputStream os = fs.create(dts);
		FileInputStream is = new FileInputStream("/Users/xmy/Desktop/a.txt");
		IOUtils.copyBytes(is, os, conf);
	}

	// 封装层写法(上传)
	@Test
	public void upload_v2() throws IOException {
//		fs.copyFromLocalFile(new Path("/Users/xmy/Desktop/flow/HTTP_20130313143750.dat"),new Path("hdfs://ns1/flow/data/"));
		fs.copyFromLocalFile(new Path("/Users/xmy/Desktop/ncmdp_08500001_Net_20130515164000.dat"),new Path("hdfs://ns1/ANA/data/"));
//		fs.copyFromLocalFile(new Path("/Users/xmy/Desktop/wc/srcdata/data.log"),new Path("hdfs://ns1/wc/srcdata/"));
	}

	// 底层写法(下载)
	public static void mains(String[] args) throws IOException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://ns1/");
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream is = fs.open(new Path("/hadoop-2.4.1.tar.gz"));
		FileOutputStream os = new FileOutputStream("/Users/xmy/Desktop/hadoop-2.4.1.tar.gz");
		IOUtils.copyBytes(is, os, conf);
	}

	// 封装层写法(下载)
	@Test
	public void download() throws IOException {
//		fs.copyToLocalFile(new Path("hdfs://ns1/ANA/output/uninet"), new Path("/Users/xmy/Desktop/uninet.dat"));
		fs.copyToLocalFile(new Path("hdfs://ns1/ANA/output/mobile"), new Path("/Users/xmy/Desktop/mobile.dat"));
	}

	@Test
	public void listFile() throws IOException {
		RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path("/"), true);
		while (files.hasNext()) {
			LocatedFileStatus file = files.next();
			Path path = file.getPath();
			String name = path.getName();
			System.out.println(name);
		}
		System.out.println("--------------------");
		FileStatus[] listStatus = fs.listStatus(new Path("/"));
		for (FileStatus fileStatus : listStatus) {
			String name = fileStatus.getPath().getName();
			System.out.println(name + " " + (fileStatus.isDirectory() ? "dir" : "file"));
		}
	}

	@Test
	public void mkdir() throws IOException {
//		 fs.mkdirs(new Path("/wc/srcdata"));
		fs.mkdirs(new Path("/ANA/data"));
	}

	@Test
	public void rm() throws IOException {
//		fs.delete(new Path("/flow/area"), true);
		fs.delete(new Path("/ANA/output"), true);
	}

}
