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
package org.bytesoft.bytejta.supports.springcloud.web;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.bytesoft.bytejta.TransactionImpl;
import org.bytesoft.bytejta.supports.rpc.TransactionRequestImpl;
import org.bytesoft.bytejta.supports.rpc.TransactionResponseImpl;
import org.bytesoft.bytejta.supports.springcloud.SpringCloudBeanRegistry;
import org.bytesoft.bytejta.supports.springcloud.loadbalancer.TransactionLoadBalancerInterceptor;
import org.bytesoft.bytejta.supports.wire.RemoteCoordinator;
import org.bytesoft.common.utils.ByteUtils;
import org.bytesoft.common.utils.CommonUtils;
import org.bytesoft.transaction.TransactionBeanFactory;
import org.bytesoft.transaction.TransactionContext;
import org.bytesoft.transaction.TransactionManager;
import org.bytesoft.transaction.archive.XAResourceArchive;
import org.bytesoft.transaction.aware.TransactionEndpointAware;
import org.bytesoft.transaction.supports.rpc.TransactionInterceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpRequest;
import org.springframework.http.client.ClientHttpRequestExecution;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.web.client.HttpClientErrorException;

import com.netflix.loadbalancer.Server;
import com.netflix.loadbalancer.Server.MetaInfo;

public class TransactionRequestInterceptor
		implements ClientHttpRequestInterceptor, TransactionEndpointAware, ApplicationContextAware {
	static final Logger logger = LoggerFactory.getLogger(TransactionRequestInterceptor.class);

	static final String HEADER_TRANCACTION_KEY = "org.bytesoft.bytejta.transaction";
	static final String HEADER_PROPAGATION_KEY = "org.bytesoft.bytejta.propagation";

	private String identifier;
	private ApplicationContext applicationContext;

	public ClientHttpResponse intercept(final HttpRequest httpRequest, byte[] body, ClientHttpRequestExecution execution)
			throws IOException {

		SpringCloudBeanRegistry beanRegistry = SpringCloudBeanRegistry.getInstance();
		TransactionBeanFactory beanFactory = beanRegistry.getBeanFactory();
		TransactionManager transactionManager = beanFactory.getTransactionManager();

		TransactionImpl transaction = //
				(TransactionImpl) transactionManager.getTransactionQuietly();

		URI uri = httpRequest.getURI();

		String path = uri.getPath();
		if (path.startsWith("/org/bytesoft/bytejta")) {
			return execution.execute(httpRequest, body);
		} else if (transaction == null) {
			return execution.execute(httpRequest, body);
		}

		final Map<String, XAResourceArchive> participants = transaction.getParticipantMap();
		beanRegistry.setLoadBalancerInterceptor(new TransactionLoadBalancerInterceptor() {
			public List<Server> beforeCompletion(List<Server> servers) {
				final List<Server> readyServerList = new ArrayList<Server>();
				final List<Server> unReadyServerList = new ArrayList<Server>();

				for (int i = 0; servers != null && i < servers.size(); i++) {
					Server server = servers.get(i);
					MetaInfo metaInfo = server.getMetaInfo();
					String instanceId = metaInfo.getInstanceId();

					if (participants.containsKey(instanceId)) {
						List<Server> serverList = new ArrayList<Server>();
						serverList.add(server);
						return serverList;
					} // end-if (participants.containsKey(instanceId))

					if (server.isReadyToServe()) {
						readyServerList.add(server);
					} else {
						unReadyServerList.add(server);
					}

				}

				logger.warn("There is no suitable server: expect= {}, actual= {}!", participants.keySet(), servers);
				return readyServerList.isEmpty() ? unReadyServerList : readyServerList;
			}

			public void afterCompletion(Server server) {
				if (server == null) {
					logger.warn(
							"There is no suitable server, the TransactionInterceptor.beforeSendRequest() operation is not executed!");
					return;
				} else {
					try {
						MetaInfo metaInfo = server.getMetaInfo();
						String instanceId = metaInfo.getInstanceId();
						invokeBeforeSendRequest(httpRequest, instanceId);
					} catch (IOException ex) {
						throw new RuntimeException(ex);
					}
				}
			}
		});

		ClientHttpResponse httpResponse = null;
		boolean serverFlag = true;
		try {
			httpResponse = execution.execute(httpRequest, body);
			return httpResponse;
		} catch (HttpClientErrorException clientEx) {
			serverFlag = false;
			throw clientEx;
		} finally {
			beanRegistry.removeLoadBalancerInterceptor();

			if (httpResponse != null) {
				this.invokeAfterRecvResponse(httpResponse, serverFlag);
			} // end-if (httpResponse != null)

		}

	}

	private void invokeBeforeSendRequest(HttpRequest httpRequest, String identifier) throws IOException {
		SpringCloudBeanRegistry beanRegistry = SpringCloudBeanRegistry.getInstance();
		TransactionBeanFactory beanFactory = beanRegistry.getBeanFactory();
		TransactionManager transactionManager = beanFactory.getTransactionManager();
		TransactionInterceptor transactionInterceptor = beanFactory.getTransactionInterceptor();

		TransactionImpl transaction = //
				(TransactionImpl) transactionManager.getTransactionQuietly();

		TransactionContext transactionContext = transaction.getTransactionContext();

		byte[] reqByteArray = CommonUtils.serializeObject(transactionContext);
		String reqTransactionStr = ByteUtils.byteArrayToString(reqByteArray);

		HttpHeaders reqHeaders = httpRequest.getHeaders();
		reqHeaders.add(HEADER_TRANCACTION_KEY, reqTransactionStr);
		reqHeaders.add(HEADER_PROPAGATION_KEY, this.identifier);

		TransactionRequestImpl request = new TransactionRequestImpl();
		request.setTransactionContext(transactionContext);
		RemoteCoordinator coordinator = beanRegistry.getConsumeCoordinator(identifier);
		request.setTargetTransactionCoordinator(coordinator);

		transactionInterceptor.beforeSendRequest(request);
	}

	private void invokeAfterRecvResponse(ClientHttpResponse httpResponse, boolean serverFlag) throws IOException {
		SpringCloudBeanRegistry beanRegistry = SpringCloudBeanRegistry.getInstance();
		TransactionBeanFactory beanFactory = beanRegistry.getBeanFactory();
		TransactionInterceptor transactionInterceptor = beanFactory.getTransactionInterceptor();

		HttpHeaders respHeaders = httpResponse.getHeaders();
		String respTransactionStr = respHeaders.getFirst(HEADER_TRANCACTION_KEY);
		String respPropagationStr = respHeaders.getFirst(HEADER_PROPAGATION_KEY);

		byte[] byteArray = ByteUtils.stringToByteArray(StringUtils.trimToNull(respTransactionStr));
		TransactionContext serverContext = (TransactionContext) CommonUtils.deserializeObject(byteArray);

		TransactionResponseImpl txResp = new TransactionResponseImpl();
		txResp.setTransactionContext(serverContext);
		RemoteCoordinator serverCoordinator = beanRegistry.getConsumeCoordinator(respPropagationStr);
		txResp.setSourceTransactionCoordinator(serverCoordinator);
		txResp.setParticipantDelistFlag(serverFlag ? false : true);

		transactionInterceptor.afterReceiveResponse(txResp);
	}

	public ApplicationContext getApplicationContext() {
		return applicationContext;
	}

	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}

	public void setEndpoint(String identifier) {
		this.identifier = identifier;
	}

}