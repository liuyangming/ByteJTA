package org.bytesoft.bytejta.supports.springcloud.loadbalancer;

import java.util.List;

import com.netflix.loadbalancer.Server;

public interface TransactionLoadBalancerInterceptor {

	public List<Server> beforeCompletion(List<Server> servers);

	public void afterCompletion(Server server);

}
