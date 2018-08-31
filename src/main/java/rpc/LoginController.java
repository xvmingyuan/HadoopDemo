package rpc;


import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
// 远程过程调用(RPC) 包名必须一致(当前包名:rpc 必须和远程服务包名一致)
public class LoginController {
	public static void main(String[] args) throws Exception {
		LoginService proxy = RPC.getProxy(LoginService.class, 1L,  new InetSocketAddress("it01",10000), new Configuration());
		String login = proxy.login("你 登录Liunx 成功了吗?", "123123");
		System.out.println(login);
	}
}
