package flowsum;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FlowSumReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
	@Override
	protected void reduce(Text key, Iterable<FlowBean> value, Context context)
			throws IOException, InterruptedException {
		long up_flowcount = 0;
		long d_flowcount = 0;
		for (FlowBean flowBean : value) {
			up_flowcount += flowBean.getUp_flow();
			d_flowcount += flowBean.getD_flow();
		}
		context.write(key, new FlowBean(key.toString(), up_flowcount, d_flowcount));

	}

}
