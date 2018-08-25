package hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Hello world!
 *
 */
public class App {
	String a ;
//	public static <E> void main(String[] args) throws IOException {
//		List<E> arrayList = new ArrayList<E>();
//		CopyOnWriteArrayList<String> a;
//		ConcurrentHashMap<Integer, String> b;
//		//downfile a file from hdfs
//		App app = new App();
//		app.a="1";
//		app.ac(app);
//		app.acs(app.a);
//		System.out.println(app.a);
////		Configuration conf = new Configuration();
////		
////		FileSystem fs = FileSystem.get(conf);
////		
////		Path src = new Path("hdfs://it01:9000/jdk-8u171-linux-x64.tar.gz");
////		
////		FSDataInputStream in = fs.open(src);
//		
//		
//	}
	public void ac(App app) {
		app.a="2";
	}
	public void acs(String a) {
		a="2";
	}
	
	public static void main(String[] args) {
		HashMap<String,String> map = new HashMap<String, String>();
		map.put("1", "one");
		map.put("2", "2");
		map.put("3", "3");
		map.put("4", "4");
		
		String string = map.get("1");
		String string1 = map.get(null);
		
		System.out.println(string);
		System.out.println(string1);
		
	}
}
