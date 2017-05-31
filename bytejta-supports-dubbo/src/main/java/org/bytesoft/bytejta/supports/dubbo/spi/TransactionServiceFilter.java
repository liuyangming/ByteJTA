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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.Map;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.apache.commons.lang3.StringUtils;
import org.bytesoft.bytejta.TransactionCoordinator;
import org.bytesoft.bytejta.supports.dubbo.DubboRemoteCoordinator;
import org.bytesoft.bytejta.supports.dubbo.InvocationContext;
import org.bytesoft.bytejta.supports.dubbo.TransactionBeanRegistry;
import org.bytesoft.bytejta.supports.rpc.TransactionRequestImpl;
import org.bytesoft.bytejta.supports.rpc.TransactionResponseImpl;
import org.bytesoft.bytejta.supports.wire.RemoteCoordinator;
import org.bytesoft.bytejta.supports.wire.RemoteCoordinatorRegistry;
import org.bytesoft.common.utils.ByteUtils;
import org.bytesoft.transaction.Transaction;
import org.bytesoft.transaction.TransactionBeanFactory;
import org.bytesoft.transaction.TransactionContext;
import org.bytesoft.transaction.TransactionManager;
import org.bytesoft.transaction.TransactionRepository;
import org.bytesoft.transaction.supports.rpc.TransactionInterceptor;
import org.bytesoft.transaction.xa.TransactionXid;
import org.bytesoft.transaction.xa.XidFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.com.caucho.hessian.io.HessianHandle;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.rpc.Filter;
import com.alibaba.dubbo.rpc.Invocation;
import com.alibaba.dubbo.rpc.Invoker;
import com.alibaba.dubbo.rpc.Result;
import com.alibaba.dubbo.rpc.RpcContext;
import com.alibaba.dubbo.rpc.RpcException;
import com.alibaba.dubbo.rpc.RpcResult;
import com.caucho.hessian.io.HessianInput;
import com.caucho.hessian.io.HessianOutput;

public class TransactionServiceFilter implements Filter {
	static final Logger logger = LoggerFactory.getLogger(TransactionServiceFilter.class);

	public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
		if (RpcContext.getContext().isProviderSide()) {
			return this.providerInvoke(invoker, invocation);
		} else {
			return this.consumerInvoke(invoker, invocation);
		}
	}

	public Result providerInvoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
		URL url = RpcContext.getContext().getUrl();
		String interfaceClazz = url.getServiceInterface();
		if (XAResource.class.getName().equals(interfaceClazz) || RemoteCoordinator.class.getName().equals(interfaceClazz)) {
			return this.providerInvokeForJTA(invoker, invocation);
		} else {
			return this.providerInvokeForSVC(invoker, invocation);
		}
	}

	public Result providerInvokeForJTA(Invoker<?> invoker, Invocation invocation) throws RpcException {
		TransactionBeanRegistry beanRegistry = TransactionBeanRegistry.getInstance();
		TransactionBeanFactory beanFactory = beanRegistry.getBeanFactory();
		XidFactory xidFactory = beanFactory.getXidFactory();
		TransactionRepository transactionRepository = beanFactory.getTransactionRepository();
		RemoteCoordinator transactionCoordinator = beanFactory.getTransactionCoordinator();

		Class<?>[] parameterTypeArray = invocation.getParameterTypes();
		Class<?> parameterType = (parameterTypeArray == null || parameterTypeArray.length == 0) ? null : parameterTypeArray[0];
		if (parameterTypeArray == null || parameterTypeArray.length == 0) {
			return this.wrapResultForProvider(invoker, invocation, false);
		} else if (Xid.class.equals(parameterType) == false) {
			return this.wrapResultForProvider(invoker, invocation, false);
		}

		RpcResult result = new RpcResult();

		Object[] arguments = invocation.getArguments();
		Xid xid = (Xid) arguments[0];

		TransactionXid globalXid = xidFactory.createGlobalXid(xid.getGlobalTransactionId());
		Transaction transaction = transactionRepository.getTransaction(globalXid);
		if (transaction == null) {
			InvocationResult wrapped = new InvocationResult();
			wrapped.setError(new XAException(XAException.XAER_NOTA));
			wrapped.setVariable(TransactionCoordinator.class.getName(), transactionCoordinator.getIdentifier());

			result.setException(null);
			result.setValue(wrapped);
		} else {
			TransactionContext transactionContext = transaction.getTransactionContext();
			String propagatedBy = String.valueOf(transactionContext.getPropagatedBy());

			String remoteAddr = invocation.getAttachment(TransactionCoordinator.class.getName());

			if (StringUtils.equals(propagatedBy, remoteAddr)) {
				return this.wrapResultForProvider(invoker, invocation, false);
			}

			InvocationResult wrapped = new InvocationResult();
			wrapped.setError(new XAException(XAException.XAER_PROTO));
			wrapped.setVariable(TransactionCoordinator.class.getName(), transactionCoordinator.getIdentifier());

			result.setException(null);
			result.setValue(wrapped);

			logger.warn("{}| branch should be invoked by its own coordinator.", globalXid);
		}

		return result;
	}

	public Result providerInvokeForSVC(Invoker<?> invoker, Invocation invocation) throws RpcException {
		RemoteCoordinatorRegistry remoteCoordinatorRegistry = RemoteCoordinatorRegistry.getInstance();
		TransactionBeanRegistry beanRegistry = TransactionBeanRegistry.getInstance();
		RemoteCoordinator consumeCoordinator = beanRegistry.getConsumeCoordinator();

		URL targetUrl = invoker.getUrl();
		String targetAddr = targetUrl.getIp();
		int targetPort = targetUrl.getPort();
		String address = String.format("%s:%s", targetAddr, targetPort);
		InvocationContext invocationContext = new InvocationContext();
		invocationContext.setServerHost(targetAddr);
		invocationContext.setServerPort(targetPort);

		RemoteCoordinator remoteCoordinator = remoteCoordinatorRegistry.getTransactionManagerStub(address);
		if (remoteCoordinator == null) {
			DubboRemoteCoordinator dubboCoordinator = new DubboRemoteCoordinator();
			dubboCoordinator.setInvocationContext(invocationContext);
			dubboCoordinator.setRemoteCoordinator(consumeCoordinator);

			remoteCoordinator = (RemoteCoordinator) Proxy.newProxyInstance(DubboRemoteCoordinator.class.getClassLoader(),
					new Class[] { RemoteCoordinator.class }, dubboCoordinator);
			remoteCoordinatorRegistry.putTransactionManagerStub(address, remoteCoordinator);
		}

		TransactionRequestImpl request = new TransactionRequestImpl();
		request.setTargetTransactionCoordinator(remoteCoordinator);

		TransactionResponseImpl response = new TransactionResponseImpl();
		response.setSourceTransactionCoordinator(remoteCoordinator);

		boolean failure = false;
		try {
			this.beforeProviderInvokeForSVC(invocation, request, response);
			return this.wrapResultForProvider(invoker, invocation, true);
		} catch (RpcException rex) {
			failure = true;

			return this.createErrorResultForProvider(rex, true);
		} catch (Throwable rex) {
			failure = true;
			logger.error("Error occurred in remote call!", rex);

			return this.createErrorResultForProvider(rex, true);
		} finally {
			try {
				this.afterProviderInvokeForSVC(invocation, request, response);
			} catch (RpcException rex) {
				if (failure) {
					logger.error("Error occurred in remote call!", rex);
				} else {
					return this.createErrorResultForProvider(rex, true);
				}
			} catch (Throwable rex) {
				if (failure) {
					logger.error("Error occurred in remote call!", rex);
				} else {
					return this.createErrorResultForProvider(rex, true);
				}
			}
		}

	}

	public Result wrapResultForProvider(Invoker<?> invoker, Invocation invocation, boolean attachRequired) {

		try {
			RpcResult result = (RpcResult) invoker.invoke(invocation);
			if (result.hasException()) {
				return this.createErrorResultForProvider(result.getException(), attachRequired);
			} else {
				return this.convertResultForProvider(result, attachRequired);
			}
		} catch (Throwable rex) {
			return this.createErrorResultForProvider(rex, attachRequired);
		}

	}

	private Result convertResultForProvider(RpcResult result, boolean attachRequired) {
		TransactionBeanRegistry beanRegistry = TransactionBeanRegistry.getInstance();
		TransactionBeanFactory beanFactory = beanRegistry.getBeanFactory();
		TransactionManager transactionManager = beanFactory.getTransactionManager();

		Object value = result.getValue();

		InvocationResult wrapped = new InvocationResult();
		wrapped.setValue(value);
		if (attachRequired) {
			Transaction transaction = transactionManager.getTransactionQuietly();
			TransactionContext transactionContext = transaction.getTransactionContext();
			wrapped.setVariable(RemoteCoordinator.class.getName(), String.valueOf(transactionContext.getPropagatedBy()));
		}

		result.setException(null);
		result.setValue(wrapped);

		return result;
	}

	private Result createErrorResultForProvider(Throwable throwable, boolean attachRequired) {
		TransactionBeanRegistry beanRegistry = TransactionBeanRegistry.getInstance();
		TransactionBeanFactory beanFactory = beanRegistry.getBeanFactory();
		RemoteCoordinator transactionCoordinator = beanFactory.getTransactionCoordinator();

		RpcResult result = new RpcResult();

		InvocationResult wrapped = new InvocationResult();
		wrapped.setError(throwable);
		if (attachRequired) {
			wrapped.setVariable(TransactionCoordinator.class.getName(), transactionCoordinator.getIdentifier());
		}

		result.setException(null);
		result.setValue(wrapped);

		return result;
	}

	private void beforeProviderInvokeForSVC(Invocation invocation, TransactionRequestImpl request,
			TransactionResponseImpl response) {
		TransactionBeanRegistry beanRegistry = TransactionBeanRegistry.getInstance();
		TransactionBeanFactory beanFactory = beanRegistry.getBeanFactory();
		TransactionInterceptor transactionInterceptor = beanFactory.getTransactionInterceptor();

		RpcException rpcError = null;
		String transactionContextContent = invocation.getAttachment(TransactionContext.class.getName());
		String propagatedBy = invocation.getAttachment(TransactionCoordinator.class.getName());
		if (StringUtils.isNotBlank(transactionContextContent)) {
			byte[] requestByteArray = ByteUtils.stringToByteArray(transactionContextContent);
			ByteArrayInputStream bais = new ByteArrayInputStream(requestByteArray);
			HessianInput input = new HessianInput(bais);
			try {
				TransactionContext remoteTransactionContext = (TransactionContext) input.readObject();
				remoteTransactionContext.setPropagatedBy(propagatedBy);
				request.setTransactionContext(remoteTransactionContext);
			} catch (IOException ex) {
				logger.error("Error occurred in remote call!", ex);
				rpcError = new RpcException("Error occurred in remote call!", ex);
			}
		}

		try {
			transactionInterceptor.afterReceiveRequest(request);
		} catch (RuntimeException rex) {
			logger.error("Error occurred in remote call!", rex);
			throw new RpcException("Error occurred in remote call!", rex);
		}

		if (rpcError != null) {
			throw rpcError;
		}

	}

	private void afterProviderInvokeForSVC(Invocation invocation, TransactionRequestImpl request,
			TransactionResponseImpl response) {
		TransactionBeanRegistry beanRegistry = TransactionBeanRegistry.getInstance();
		TransactionBeanFactory beanFactory = beanRegistry.getBeanFactory();
		TransactionInterceptor transactionInterceptor = beanFactory.getTransactionInterceptor();
		TransactionManager transactionManager = beanFactory.getTransactionManager();

		Transaction transaction = transactionManager.getTransactionQuietly();
		TransactionContext nativeTransactionContext = transaction == null ? null : transaction.getTransactionContext();

		response.setTransactionContext(nativeTransactionContext);
		try {
			transactionInterceptor.beforeSendResponse(response);
		} catch (RuntimeException rex) {
			logger.error("Error occurred in remote call!", rex);
			throw new RpcException("Error occurred in remote call!", rex);
		}
	}

	public Result consumerInvoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
		URL url = RpcContext.getContext().getUrl();
		String interfaceClazz = url.getServiceInterface();
		if (XAResource.class.getName().equals(interfaceClazz) || RemoteCoordinator.class.getName().equals(interfaceClazz)) {
			return this.consumerInvokeForJTA(invoker, invocation);
		} else {
			return this.consumerInvokeForSVC(invoker, invocation);
		}
	}

	public Result consumerInvokeForJTA(Invoker<?> invoker, Invocation invocation) throws RpcException {
		TransactionBeanRegistry beanRegistry = TransactionBeanRegistry.getInstance();
		TransactionBeanFactory beanFactory = beanRegistry.getBeanFactory();
		RemoteCoordinator transactionCoordinator = beanFactory.getTransactionCoordinator();

		Map<String, String> attachments = invocation.getAttachments();
		attachments.put(TransactionCoordinator.class.getName(), transactionCoordinator.getIdentifier());
		return invoker.invoke(invocation);
	}

	public Result consumerInvokeForSVC(Invoker<?> invoker, Invocation invocation) throws RpcException {
		RemoteCoordinatorRegistry remoteCoordinatorRegistry = RemoteCoordinatorRegistry.getInstance();
		TransactionBeanRegistry beanRegistry = TransactionBeanRegistry.getInstance();
		TransactionBeanFactory beanFactory = beanRegistry.getBeanFactory();
		RemoteCoordinator transactionCoordinator = beanFactory.getTransactionCoordinator();
		RemoteCoordinator consumeCoordinator = beanRegistry.getConsumeCoordinator();
		TransactionManager transactionManager = beanFactory.getTransactionManager();
		Transaction transaction = transactionManager.getTransactionQuietly();
		TransactionContext nativeTransactionContext = transaction == null ? null : transaction.getTransactionContext();

		URL targetUrl = invoker.getUrl();
		String targetAddr = targetUrl.getIp();
		int targetPort = targetUrl.getPort();
		String address = String.format("%s:%s", targetAddr, targetPort);
		InvocationContext invocationContext = new InvocationContext();
		invocationContext.setServerHost(targetAddr);
		invocationContext.setServerPort(targetPort);

		RemoteCoordinator remoteCoordinator = remoteCoordinatorRegistry.getTransactionManagerStub(address);
		if (remoteCoordinator == null) {
			DubboRemoteCoordinator dubboCoordinator = new DubboRemoteCoordinator();
			dubboCoordinator.setInvocationContext(invocationContext);
			dubboCoordinator.setRemoteCoordinator(consumeCoordinator);

			remoteCoordinator = (RemoteCoordinator) Proxy.newProxyInstance(DubboRemoteCoordinator.class.getClassLoader(),
					new Class[] { RemoteCoordinator.class }, dubboCoordinator);
			remoteCoordinatorRegistry.putTransactionManagerStub(address, remoteCoordinator);
		}

		TransactionRequestImpl request = new TransactionRequestImpl();
		request.setTransactionContext(nativeTransactionContext);
		request.setTargetTransactionCoordinator(remoteCoordinator);

		TransactionResponseImpl response = new TransactionResponseImpl();
		response.setSourceTransactionCoordinator(remoteCoordinator);
		boolean success = true;
		try {
			this.beforeConsumerInvokeForSVC(invocation, request, response);
			RpcResult result = (RpcResult) invoker.invoke(invocation);
			Object value = result.getValue();
			if (InvocationResult.class.isInstance(value)) {
				InvocationResult wrapped = (InvocationResult) value;
				result.setValue(null);
				result.setException(null);

				if (wrapped.isFailure()) {
					result.setException(wrapped.getError());
				} else {
					result.setValue(wrapped.getValue());
				}

				String propagatedBy = (String) wrapped.getVariable(RemoteCoordinator.class.getName());
				String identifier = transactionCoordinator.getIdentifier();
				boolean participantDelistRequired = StringUtils.equals(propagatedBy, identifier) == false;
				response.setParticipantDelistFlag(participantDelistRequired);
				response.setParticipantEnlistFlag(request.isParticipantEnlistFlag());
			}
			return result;
		} catch (RpcException rex) {
			success = false;

			RpcResult result = new RpcResult();
			result.setException(rex);
			return result;
		} catch (Throwable rex) {
			success = false;
			logger.error("Error occurred in remote call!", rex);

			RpcResult result = new RpcResult();
			result.setException(new RpcException("Error occurred in remote call!", rex));
			return result;
		} finally {
			try {
				this.afterConsumerInvokeForSVC(invocation, request, response);
			} catch (RpcException rex) {
				if (success) {
					RpcResult result = new RpcResult();
					result.setException(rex);
					return result;
				}
			} catch (RuntimeException rex) {
				if (success) {
					logger.error("Error occurred in remote call!", rex);

					RpcResult result = new RpcResult();
					result.setException(new RpcException("Error occurred in remote call!", rex));
					return result;
				}
			}
		}

	}

	private void beforeConsumerInvokeForSVC(Invocation invocation, TransactionRequestImpl request,
			TransactionResponseImpl response) {
		TransactionBeanRegistry beanRegistry = TransactionBeanRegistry.getInstance();
		TransactionBeanFactory beanFactory = beanRegistry.getBeanFactory();
		TransactionInterceptor transactionInterceptor = beanFactory.getTransactionInterceptor();
		RemoteCoordinator transactionCoordinator = beanFactory.getTransactionCoordinator();

		transactionInterceptor.beforeSendRequest(request);
		if (request.getTransactionContext() != null) {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			HessianOutput output = new HessianOutput(baos);
			try {
				output.writeObject(request.getTransactionContext());
			} catch (IOException ex) {
				logger.error("Error occurred in remote call!", ex);
				throw new RpcException("Error occurred in remote call!", ex);
			}
			String transactionContextContent = ByteUtils.byteArrayToString(baos.toByteArray());
			Map<String, String> attachments = invocation.getAttachments();
			attachments.put(TransactionContext.class.getName(), transactionContextContent);
			attachments.put(TransactionCoordinator.class.getName(), transactionCoordinator.getIdentifier());
		}
	}

	private void afterConsumerInvokeForSVC(Invocation invocation, TransactionRequestImpl request,
			TransactionResponseImpl response) {
		TransactionBeanRegistry beanRegistry = TransactionBeanRegistry.getInstance();
		TransactionBeanFactory beanFactory = beanRegistry.getBeanFactory();
		TransactionInterceptor transactionInterceptor = beanFactory.getTransactionInterceptor();

		RpcException rpcError = null;
		try {
			if (request.getTransactionContext() != null) {
				String transactionContextContent = invocation.getAttachment(TransactionContext.class.getName());
				byte[] byteArray = ByteUtils.stringToByteArray(transactionContextContent);
				ByteArrayInputStream bais = new ByteArrayInputStream(byteArray);
				HessianInput input = new HessianInput(bais);
				TransactionContext remoteTransactionContext = (TransactionContext) input.readObject();
				response.setTransactionContext(remoteTransactionContext);
			}
		} catch (IOException ex) {
			logger.error("Error occurred in remote call!", ex);
			rpcError = new RpcException("Error occurred in remote call!", ex);
		}

		try {
			transactionInterceptor.afterReceiveResponse(response);
		} catch (RuntimeException rex) {
			logger.error("Error occurred in remote call!", rex);
			throw new RpcException("Error occurred in remote call!", rex);
		}

		if (rpcError != null) {
			throw rpcError;
		}

	}

	static class InvocationResult implements HessianHandle, Serializable {
		private static final long serialVersionUID = 1L;

		private Throwable error;
		private Object value;
		private final Map<String, Serializable> variables = new HashMap<String, Serializable>();

		public boolean isFailure() {
			return this.error != null;
		}

		public Object getValue() {
			return value;
		}

		public void setValue(Object value) {
			this.value = value;
		}

		public void setVariable(String key, Serializable value) {
			this.variables.put(key, value);
		}

		public Serializable getVariable(String key) {
			return this.variables.get(key);
		}

		public Throwable getError() {
			return error;
		}

		public void setError(Throwable error) {
			this.error = error;
		}

	}

}
