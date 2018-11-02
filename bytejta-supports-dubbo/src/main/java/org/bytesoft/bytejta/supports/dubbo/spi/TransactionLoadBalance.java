/**
 * Copyright 2014-2016 yangming.liu<bytefox@126.com>.
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
package org.bytesoft.bytejta.supports.dubbo.spi;

import java.util.List;

import org.bytesoft.bytejta.TransactionImpl;
import org.bytesoft.bytejta.supports.dubbo.InvocationContextRegistry;
import org.bytesoft.bytejta.supports.dubbo.TransactionBeanRegistry;
import org.bytesoft.bytejta.supports.dubbo.ext.ILoadBalancer;
import org.bytesoft.bytejta.supports.internal.RemoteCoordinatorRegistry;
import org.bytesoft.common.utils.CommonUtils;
import org.bytesoft.transaction.TransactionBeanFactory;
import org.bytesoft.transaction.TransactionManager;
import org.bytesoft.transaction.archive.XAResourceArchive;
import org.bytesoft.transaction.remote.RemoteAddr;
import org.bytesoft.transaction.remote.RemoteNode;
import org.bytesoft.transaction.supports.resource.XAResourceDescriptor;
import org.springframework.core.env.Environment;

import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.extension.ExtensionLoader;
import com.alibaba.dubbo.rpc.Invocation;
import com.alibaba.dubbo.rpc.Invoker;
import com.alibaba.dubbo.rpc.RpcException;
import com.alibaba.dubbo.rpc.cluster.LoadBalance;

public final class TransactionLoadBalance implements LoadBalance {
	static final String CONSTANT_LOADBALANCE_KEY = "org.bytesoft.bytejta.loadbalance";

	private ILoadBalancer loadBalancer;

	private void fireInitializeIfNecessary() {
		if (this.loadBalancer == null) {
			this.initializeIfNecessary();
		}
	}

	private synchronized void initializeIfNecessary() {
		if (this.loadBalancer == null) {
			Environment environment = TransactionBeanRegistry.getInstance().getEnvironment();
			String loadBalanceKey = environment.getProperty(CONSTANT_LOADBALANCE_KEY, "default");
			ExtensionLoader<ILoadBalancer> extensionLoader = ExtensionLoader.getExtensionLoader(ILoadBalancer.class);
			this.loadBalancer = extensionLoader.getExtension(loadBalanceKey);
		}
	}

	public <T> Invoker<T> select(List<Invoker<T>> invokers, URL url, Invocation invocation) throws RpcException {
		InvocationContextRegistry registry = InvocationContextRegistry.getInstance();
		RemoteNode invocationContext = registry.getInvocationContext();
		if (invocationContext == null) {
			return this.selectConfigedInvoker(invokers, url, invocation);
		} else {
			return this.selectSpecificInvoker(invokers, url, invocation, invocationContext);
		}
	}

	public <T> Invoker<T> selectConfigedInvoker(List<Invoker<T>> invokers, URL url, Invocation invocation) throws RpcException {
		if (invokers == null || invokers.isEmpty()) {
			throw new RpcException("No invoker is found!");
		}

		TransactionBeanFactory beanFactory = TransactionBeanRegistry.getInstance().getBeanFactory();
		RemoteCoordinatorRegistry participantRegistry = RemoteCoordinatorRegistry.getInstance();
		TransactionManager transactionManager = beanFactory.getTransactionManager();
		TransactionImpl transaction = //
				(TransactionImpl) transactionManager.getTransactionQuietly();
		List<XAResourceArchive> participantList = transaction == null ? null : transaction.getRemoteParticipantList();

		RemoteAddr instanceAddr = null;
		for (int i = 0; invokers != null && participantList != null && participantList.isEmpty() == false
				&& i < invokers.size(); i++) {
			Invoker<T> invoker = invokers.get(i);
			URL invokerUrl = invoker.getUrl();
			RemoteAddr invokerAddr = new RemoteAddr();
			invokerAddr.setServerHost(invokerUrl.getHost());
			invokerAddr.setServerPort(invokerUrl.getPort());

			RemoteNode remoteNode = participantRegistry.getRemoteNode(invokerAddr);
			if (remoteNode == null) {
				continue;
			}

			XAResourceDescriptor participant = transaction.getRemoteCoordinator(remoteNode.getServiceKey());
			if (participant == null) {
				continue;
			}

			String identifier = participant.getIdentifier();
			RemoteAddr remoteAddr = CommonUtils.getRemoteAddr(identifier);
			if (invokerAddr.equals(remoteAddr) == false) {
				instanceAddr = remoteAddr;
				continue;
			}

			return invoker;
		}

		if (instanceAddr != null) {
			throw new RpcException(
					String.format("Invoker(%s:%s) is not found!", instanceAddr.getServerHost(), instanceAddr.getServerPort()));
		}

		this.fireInitializeIfNecessary();

		if (this.loadBalancer == null) {
			throw new RpcException("No org.bytesoft.bytejta.supports.dubbo.ext.ILoadBalancer is found!");
		} else {
			return this.loadBalancer.select(invokers, url, invocation);
		}

	}

	public <T> Invoker<T> selectSpecificInvoker(List<Invoker<T>> invokers, URL url, Invocation invocation, RemoteNode context)
			throws RpcException {
		RemoteAddr remoteAddr = new RemoteAddr();
		remoteAddr.setServerHost(context.getServerHost());
		remoteAddr.setServerPort(context.getServerPort());
		for (int i = 0; invokers != null && i < invokers.size(); i++) {
			Invoker<T> invoker = invokers.get(i);
			URL targetUrl = invoker.getUrl();
			RemoteAddr targetAddr = new RemoteAddr();
			targetAddr.setServerHost(targetUrl.getIp());
			targetAddr.setServerPort(targetUrl.getPort());
			if (targetAddr.equals(remoteAddr)) {
				return invoker;
			} // end-if (targetAddr.equals(remoteAddr))
		}

		throw new RpcException(String.format("Invoker(%s:%s) is not found!", context.getServerHost(), context.getServerPort()));
	}

}
