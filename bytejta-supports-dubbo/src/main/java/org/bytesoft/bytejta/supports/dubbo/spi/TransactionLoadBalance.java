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
import java.util.Random;

import org.apache.commons.lang3.StringUtils;
import org.bytesoft.bytejta.TransactionImpl;
import org.bytesoft.bytejta.supports.dubbo.InvocationContext;
import org.bytesoft.bytejta.supports.dubbo.InvocationContextRegistry;
import org.bytesoft.bytejta.supports.dubbo.TransactionBeanRegistry;
import org.bytesoft.transaction.TransactionBeanFactory;
import org.bytesoft.transaction.TransactionManager;
import org.bytesoft.transaction.archive.XAResourceArchive;
import org.bytesoft.transaction.supports.resource.XAResourceDescriptor;

import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.rpc.Invocation;
import com.alibaba.dubbo.rpc.Invoker;
import com.alibaba.dubbo.rpc.RpcException;
import com.alibaba.dubbo.rpc.cluster.LoadBalance;

public final class TransactionLoadBalance implements LoadBalance {
	static final Random random = new Random();

	public <T> Invoker<T> select(List<Invoker<T>> invokers, URL url, Invocation invocation) throws RpcException {
		InvocationContextRegistry registry = InvocationContextRegistry.getInstance();
		InvocationContext invocationContext = registry.getInvocationContext();
		if (invocationContext == null) {
			return this.selectRandomInvoker(invokers, url, invocation);
		} else {
			return this.selectSpecificInvoker(invokers, url, invocation, invocationContext);
		}
	}

	public <T> Invoker<T> selectRandomInvoker(List<Invoker<T>> invokers, URL url, Invocation invocation) throws RpcException {
		int lengthOfInvokerList = invokers == null ? 0 : invokers.size();
		if (invokers == null || invokers.isEmpty()) {
			throw new RpcException("No invoker is found!");
		}

		TransactionBeanFactory beanFactory = TransactionBeanRegistry.getInstance().getBeanFactory();
		TransactionManager transactionManager = beanFactory.getTransactionManager();
		TransactionImpl transaction = //
				(TransactionImpl) transactionManager.getTransactionQuietly();
		List<XAResourceArchive> participantList = transaction == null ? null : transaction.getRemoteParticipantList();

		for (int i = 0; invokers != null && participantList != null && i < invokers.size(); i++) {
			Invoker<T> invoker = invokers.get(i);
			URL invokerUrl = invoker.getUrl();
			String invokerHost = invokerUrl.getHost();
			String invokerName = invokerUrl.getParameter("application");
			int invokerPort = invokerUrl.getPort();
			String invokerAddr = String.format("%s:%s:%s", invokerHost, invokerName, invokerPort);
			for (int j = 0; participantList != null && j < participantList.size(); j++) {
				XAResourceArchive archive = participantList.get(j);
				XAResourceDescriptor descriptor = archive.getDescriptor();
				String identifier = descriptor.getIdentifier();
				if (StringUtils.equalsIgnoreCase(invokerAddr, identifier)) {
					return invoker;
				} // end-if (StringUtils.equalsIgnoreCase(invokerAddr, identifier))
			}
		}

		return invokers.get(random.nextInt(lengthOfInvokerList));
	}

	public <T> Invoker<T> selectSpecificInvoker(List<Invoker<T>> invokers, URL url, Invocation invocation,
			InvocationContext context) throws RpcException {
		String serverHost = context.getServerHost();
		int serverPort = context.getServerPort();
		for (int i = 0; invokers != null && i < invokers.size(); i++) {
			Invoker<T> invoker = invokers.get(i);
			URL targetUrl = invoker.getUrl();
			String targetAddr = targetUrl.getIp();
			int targetPort = targetUrl.getPort();
			if (StringUtils.equals(targetAddr, serverHost) && targetPort == serverPort) {
				return invoker;
			}
		}
		throw new RpcException(String.format("Invoker(%s:%s) is not found!", serverHost, serverPort));
	}
}
