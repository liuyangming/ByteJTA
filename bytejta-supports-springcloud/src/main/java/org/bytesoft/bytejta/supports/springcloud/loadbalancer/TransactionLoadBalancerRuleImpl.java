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
package org.bytesoft.bytejta.supports.springcloud.loadbalancer;

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.bytesoft.bytejta.supports.springcloud.SpringCloudBeanRegistry;
import org.bytesoft.bytejta.supports.springcloud.rule.TransactionRule;
import org.bytesoft.bytejta.supports.springcloud.rule.TransactionRuleImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.env.Environment;

import com.netflix.client.config.IClientConfig;
import com.netflix.loadbalancer.AbstractLoadBalancerRule;
import com.netflix.loadbalancer.ILoadBalancer;
import com.netflix.loadbalancer.IRule;
import com.netflix.loadbalancer.Server;

public class TransactionLoadBalancerRuleImpl extends AbstractLoadBalancerRule implements IRule {
	static final String CONSTANT_RULE_KEY = "org.bytesoft.bytejta.NFTransactionRuleClassName";
	static Logger logger = LoggerFactory.getLogger(TransactionLoadBalancerRuleImpl.class);

	static Class<?> transactionRuleClass;

	private IClientConfig clientConfig;

	public Server choose(Object key) {
		SpringCloudBeanRegistry registry = SpringCloudBeanRegistry.getInstance();
		TransactionLoadBalancerInterceptor interceptor = registry.getLoadBalancerInterceptor();

		if (transactionRuleClass == null) {
			Environment environment = registry.getEnvironment();
			String clazzName = environment.getProperty(CONSTANT_RULE_KEY);
			if (StringUtils.isNotBlank(clazzName)) {
				try {
					ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
					transactionRuleClass = classLoader.loadClass(clazzName);
				} catch (Exception ex) {
					logger.error("Error occurred while loading class {}.", clazzName, ex);
					transactionRuleClass = TransactionRuleImpl.class;
				}
			} else {
				transactionRuleClass = TransactionRuleImpl.class;
			}
		}

		TransactionRule transactionRule = null;
		if (TransactionRuleImpl.class.equals(transactionRuleClass)) {
			transactionRule = new TransactionRuleImpl();
		} else {
			try {
				transactionRule = (TransactionRule) transactionRuleClass.newInstance();
			} catch (Exception ex) {
				logger.error("Can not create an instance of class {}.", transactionRuleClass.getName(), ex);
				transactionRule = new TransactionRuleImpl();
			}
		}
		transactionRule.initWithNiwsConfig(this.clientConfig);
		transactionRule.setLoadBalancer(this.getLoadBalancer());

		if (interceptor == null) {
			return transactionRule.chooseServer(key);
		} // end-if (interceptor == null)

		ILoadBalancer loadBalancer = this.getLoadBalancer();
		List<Server> servers = loadBalancer.getAllServers();

		Server server = null;
		try {
			List<Server> serverList = interceptor.beforeCompletion(servers);

			server = transactionRule.chooseServer(key, serverList);
		} finally {
			interceptor.afterCompletion(server);
		}

		return server;
	}

	public void initWithNiwsConfig(IClientConfig clientConfig) {
		this.clientConfig = clientConfig;
	}

	public IClientConfig getClientConfig() {
		return clientConfig;
	}

}
