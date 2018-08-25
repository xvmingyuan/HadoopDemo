package flowarea;

import java.util.HashMap;

import org.apache.hadoop.mapreduce.Partitioner;

import flowsum.FlowBean;

public class Areapartitioner2<KEY, VALUE> extends Partitioner<KEY, VALUE> {
	private static HashMap<String, Integer> areaMap = new HashMap<String, Integer>();

	static {
		areaMap.put("135", 0);
		areaMap.put("136", 1);
		areaMap.put("137", 2);
		areaMap.put("138", 3);
		areaMap.put("139", 4);
	}

	@Override
	public int getPartition(KEY key, VALUE value, int numPartitions) {
		FlowBean flowbean = new FlowBean();
		flowbean = (FlowBean) key;
		int areaCoder = areaMap.get(flowbean.getPhoneNB().substring(0, 3))==null?5:areaMap.get(flowbean.getPhoneNB().substring(0, 3));

		return areaCoder;
	}

}
