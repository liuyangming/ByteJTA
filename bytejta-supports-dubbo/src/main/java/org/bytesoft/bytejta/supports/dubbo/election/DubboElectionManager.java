/**
 * Copyright 2014-2018 yangming.liu<bytefox@126.com>.
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
package org.bytesoft.bytejta.supports.dubbo.election;

import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.bytesoft.bytejta.supports.election.AbstractElectionManager;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.EnvironmentAware;
import org.springframework.core.env.Environment;

import com.alibaba.dubbo.config.RegistryConfig;

public class DubboElectionManager extends AbstractElectionManager
		implements InitializingBean, EnvironmentAware, ApplicationContextAware {

	private CuratorFramework curatorFramework;
	private Environment environment;
	private ApplicationContext applicationContext;

	public CuratorFramework getCuratorFramework() {
		if (this.curatorFramework == null) {
			this.initCuratorFramework();
		}
		return this.curatorFramework;
	}

	public synchronized void initCuratorFramework() {
		if (this.curatorFramework == null) {
			String connectionString = null;
			try {
				RegistryConfig registryConfig = this.applicationContext.getBean(RegistryConfig.class);
				connectionString = registryConfig.getAddress();
			} catch (BeansException ex) {
				String registryAddr = this.environment.getProperty("dubbo.registry.address");
				String springAddr = this.environment.getProperty("spring.dubbo.registry.address");
				connectionString = StringUtils.isBlank(springAddr) ? registryAddr : springAddr;
			}

			if (connectionString == null) {
				throw new IllegalStateException();
			}

			String finalZookeeperAddr = null;
			if (connectionString.startsWith("zookeeper://")) {
				finalZookeeperAddr = connectionString.substring("zookeeper://".length());
			} else {
				finalZookeeperAddr = connectionString;
			}

			this.curatorFramework = CuratorFrameworkFactory.builder().connectString(finalZookeeperAddr) //
					.sessionTimeoutMs(1000 * 6).retryPolicy(new ExponentialBackoffRetry(1000, 3)).build();
			this.curatorFramework.start();
		}
	}

	public Environment getEnvironment() {
		return environment;
	}

	public void setEnvironment(Environment environment) {
		this.environment = environment;
	}

	public ApplicationContext getApplicationContext() {
		return applicationContext;
	}

	public void setApplicationContext(ApplicationContext applicationContext) {
		this.applicationContext = applicationContext;
	}

}
