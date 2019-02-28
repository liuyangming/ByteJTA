/**
 * Copyright 2014-2017 yangming.liu<bytefox@126.com>.
 *
 * This copyrighted material is made available to anyone wishing to use, modify,
 * copy, or redistribute it subject to the terms and conditions of the GNU
 * Lesser General Public License, as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this distribution; if not, see <http://www.gnu.org/licenses/>.
 */
package org.bytesoft.bytejta.supports.springcloud;

import java.lang.reflect.Proxy;

import org.apache.commons.lang3.StringUtils;
import org.bytesoft.bytejta.supports.internal.RemoteCoordinatorRegistry;
import org.bytesoft.bytejta.supports.springcloud.loadbalancer.TransactionLoadBalancerInterceptor;
import org.bytesoft.common.utils.CommonUtils;
import org.bytesoft.transaction.TransactionBeanFactory;
import org.bytesoft.transaction.aware.TransactionBeanFactoryAware;
import org.bytesoft.transaction.remote.RemoteAddr;
import org.bytesoft.transaction.remote.RemoteCoordinator;
import org.bytesoft.transaction.remote.RemoteNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.EnvironmentAware;
import org.springframework.core.env.Environment;
import org.springframework.web.client.RestTemplate;

public final class SpringCloudBeanRegistry implements TransactionBeanFactoryAware, EnvironmentAware {
	static final Logger logger = LoggerFactory.getLogger(SpringCloudBeanRegistry.class);
	private static final SpringCloudBeanRegistry instance = new SpringCloudBeanRegistry();

	@javax.inject.Inject
	private TransactionBeanFactory beanFactory;
	private RestTemplate restTemplate;
	private ThreadLocal<TransactionLoadBalancerInterceptor> interceptors = new ThreadLocal<TransactionLoadBalancerInterceptor>();
	private Environment environment;

	private SpringCloudBeanRegistry() {
		if (instance != null) {
			throw new IllegalStateException();
		}
	}

	public static SpringCloudBeanRegistry getInstance() {
		return instance;
	}

	public RemoteCoordinator getConsumeCoordinator(String identifier) {
		RemoteCoordinatorRegistry registry = RemoteCoordinatorRegistry.getInstance();
		if (StringUtils.isBlank(identifier)) {
			return null;
		}

		RemoteAddr remoteAddr = CommonUtils.getRemoteAddr(identifier);
		RemoteNode remoteNode = CommonUtils.getRemoteNode(identifier);

		RemoteNode targetNode = registry.getRemoteNode(remoteAddr);
		if (targetNode == null && remoteAddr != null //
				&& remoteNode != null && StringUtils.isNotBlank(remoteNode.getServiceKey())) {
			registry.putRemoteNode(remoteAddr, remoteNode);
		}

		// String application = CommonUtils.getApplication(identifier);
		// RemoteCoordinator participant = registry.getParticipant(application);
		// if (participant != null) {
		// return participant;
		// }

		RemoteCoordinator physical = registry.getPhysicalInstance(remoteAddr);
		if (physical != null) {
			return physical;
		}

		SpringCloudCoordinator handler = new SpringCloudCoordinator();
		handler.setIdentifier(identifier);
		handler.setEnvironment(this.environment);

		physical = (RemoteCoordinator) Proxy.newProxyInstance(SpringCloudCoordinator.class.getClassLoader(),
				new Class[] { RemoteCoordinator.class }, handler);

		registry.putPhysicalInstance(remoteAddr, physical);

		// registry.putRemoteNode(remoteAddr, remoteNode);
		// registry.putParticipant(application, participant);

		return physical;
	}

	public TransactionLoadBalancerInterceptor getLoadBalancerInterceptor() {
		return this.interceptors.get();
	}

	public void setLoadBalancerInterceptor(TransactionLoadBalancerInterceptor interceptor) {
		this.interceptors.set(interceptor);
	}

	public void removeLoadBalancerInterceptor() {
		this.interceptors.remove();
	}

	public RestTemplate getRestTemplate() {
		return restTemplate;
	}

	public void setRestTemplate(RestTemplate restTemplate) {
		this.restTemplate = restTemplate;
	}

	public void setBeanFactory(TransactionBeanFactory tbf) {
		this.beanFactory = tbf;
	}

	public TransactionBeanFactory getBeanFactory() {
		return beanFactory;
	}

	public Environment getEnvironment() {
		return environment;
	}

	public void setEnvironment(Environment environment) {
		this.environment = environment;
	}

}
