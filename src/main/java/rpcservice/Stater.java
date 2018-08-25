package rpcservice;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Builder;
import org.apache.hadoop.ipc.RPC.Server;

public class Stater {
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Builder builder = new RPC.Builder(conf);
		builder.setBindAddress("it01").setPort(10000)
				.setProtocol(LoginService.class)
				.setInstance(new LoginServiceImpl());
		Server server = builder.build();
		server.start();
	}
}
