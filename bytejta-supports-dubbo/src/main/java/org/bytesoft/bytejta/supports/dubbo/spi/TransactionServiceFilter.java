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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.apache.commons.lang3.StringUtils;
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
import org.springframework.transaction.annotation.Propagation;

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
	static final String KEY_XA_RESOURCE_START = "start";
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
		if (StringUtils.equals(invocation.getMethodName(), KEY_XA_RESOURCE_START)
				&& Arrays.equals(invocation.getParameterTypes(), new Class<?>[] { Xid.class, Integer.TYPE })) {
			return this.providerInvokeForKey(invoker, invocation);
		} else if (XAResource.class.getName().equals(interfaceClazz)) {
			return this.providerInvokeForJTA(invoker, invocation);
		} else if (RemoteCoordinator.class.getName().equals(interfaceClazz)) {
			return this.providerInvokeForJTA(invoker, invocation);
		} else {
			return this.providerInvokeForSVC(invoker, invocation);
		}
	}

	public Result providerInvokeForKey(Invoker<?> invoker, Invocation invocation) throws RpcException {
		RemoteCoordinatorRegistry coordinatorRegistry = RemoteCoordinatorRegistry.getInstance();
		TransactionBeanRegistry beanRegistry = TransactionBeanRegistry.getInstance();
		TransactionBeanFactory beanFactory = beanRegistry.getBeanFactory();
		RemoteCoordinator consumeCoordinator = beanRegistry.getConsumeCoordinator();
		RemoteCoordinator transactionCoordinator = beanFactory.getTransactionCoordinator();

		String instanceId = StringUtils.trimToEmpty(invocation.getAttachment(RemoteCoordinator.class.getName()));

		RemoteCoordinator remoteCoordinator = coordinatorRegistry.getRemoteCoordinator(instanceId);
		if (StringUtils.isNotBlank(instanceId) && remoteCoordinator == null) {
			String[] values = instanceId == null ? new String[0] : instanceId.split("\\s*:\\s*");

			String targetAddr = values.length == 3 ? values[0] : StringUtils.EMPTY;
			String targetName = values.length == 3 ? values[1] : StringUtils.EMPTY;
			String targetPort = values.length == 3 ? values[2] : String.valueOf(0);

			String remoteAddr = StringUtils.isBlank(targetAddr) && StringUtils.isBlank(targetPort) //
					? StringUtils.EMPTY : String.format("%s:%s", targetAddr, targetPort);

			InvocationContext invocationContext = new InvocationContext();
			invocationContext.setServerHost(targetAddr);
			invocationContext.setServiceKey(targetName);
			invocationContext.setServerPort(Integer.valueOf(targetPort));

			DubboRemoteCoordinator dubboCoordinator = new DubboRemoteCoordinator();
			dubboCoordinator.setInvocationContext(invocationContext);
			dubboCoordinator.setRemoteCoordinator(consumeCoordinator);

			remoteCoordinator = (RemoteCoordinator) Proxy.newProxyInstance(DubboRemoteCoordinator.class.getClassLoader(),
					new Class[] { RemoteCoordinator.class }, dubboCoordinator);

			coordinatorRegistry.putApplication(remoteAddr, targetName);
			coordinatorRegistry.putRemoteAddr(instanceId, remoteAddr);

			coordinatorRegistry.putRemoteCoordinator(instanceId, remoteCoordinator);
			coordinatorRegistry.putRemoteCoordinatorByAddr(remoteAddr, remoteCoordinator);
		}

		RpcResult result = new RpcResult();

		InvocationResult wrapped = new InvocationResult();
		wrapped.setVariable(RemoteCoordinator.class.getName(), transactionCoordinator.getIdentifier());

		result.setException(null);
		result.setValue(wrapped);

		return result;
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
			return this.wrapResultForProvider(invoker, invocation, null, false);
		} else if (Xid.class.equals(parameterType) == false) {
			return this.wrapResultForProvider(invoker, invocation, null, false);
		}

		RpcResult result = new RpcResult();

		Object[] arguments = invocation.getArguments();
		Xid xid = (Xid) arguments[0];

		TransactionXid globalXid = xidFactory.createGlobalXid(xid.getGlobalTransactionId());
		Transaction transaction = transactionRepository.getTransaction(globalXid);
		if (transaction == null) {
			InvocationResult wrapped = new InvocationResult();
			wrapped.setError(new XAException(XAException.XAER_NOTA));
			wrapped.setVariable(RemoteCoordinator.class.getName(), transactionCoordinator.getIdentifier());

			result.setException(null);
			result.setValue(wrapped);
		} else {
			TransactionContext transactionContext = transaction.getTransactionContext();
			String propagatedBy = String.valueOf(transactionContext.getPropagatedBy());

			String remoteAddr = invocation.getAttachment(RemoteCoordinator.class.getName());

			if (StringUtils.equals(propagatedBy, remoteAddr)) {
				return this.wrapResultForProvider(invoker, invocation, propagatedBy, false);
			}

			InvocationResult wrapped = new InvocationResult();
			wrapped.setError(new XAException(XAException.XAER_PROTO));

			wrapped.setVariable(Propagation.class.getName(), String.valueOf(transactionContext.getPropagatedBy()));
			wrapped.setVariable(RemoteCoordinator.class.getName(), transactionCoordinator.getIdentifier());

			result.setException(null);
			result.setValue(wrapped);

			logger.warn("{}| branch should be invoked by its own coordinator(expect= {}, actual= {})." //
					, globalXid, propagatedBy, remoteAddr);
		}

		return result;
	}

	public Result providerInvokeForSVC(Invoker<?> invoker, Invocation invocation) throws RpcException {
		RemoteCoordinatorRegistry coordinatorRegistry = RemoteCoordinatorRegistry.getInstance();
		TransactionBeanRegistry beanRegistry = TransactionBeanRegistry.getInstance();
		TransactionBeanFactory beanFactory = beanRegistry.getBeanFactory();
		TransactionManager transactionManager = beanFactory.getTransactionManager();
		RemoteCoordinator consumeCoordinator = beanRegistry.getConsumeCoordinator();

		String instanceId = invocation.getAttachment(RemoteCoordinator.class.getName());

		RemoteCoordinator remoteCoordinator = coordinatorRegistry.getRemoteCoordinator(instanceId);
		if (StringUtils.isNotBlank(instanceId) && remoteCoordinator == null) {
			String[] values = instanceId == null ? new String[0] : instanceId.split("\\s*:\\s*");

			String targetAddr = values.length == 3 ? values[0] : StringUtils.EMPTY;
			String targetName = values.length == 3 ? values[1] : StringUtils.EMPTY;
			String targetPort = values.length == 3 ? values[2] : String.valueOf(0);

			String remoteAddr = StringUtils.isBlank(targetAddr) && StringUtils.isBlank(targetPort) //
					? StringUtils.EMPTY : String.format("%s:%s", targetAddr, targetPort);

			InvocationContext invocationContext = new InvocationContext();
			invocationContext.setServerHost(targetAddr);
			invocationContext.setServiceKey(targetName);
			invocationContext.setServerPort(Integer.valueOf(targetPort));

			DubboRemoteCoordinator dubboCoordinator = new DubboRemoteCoordinator();
			dubboCoordinator.setInvocationContext(invocationContext);
			dubboCoordinator.setRemoteCoordinator(consumeCoordinator);

			remoteCoordinator = (RemoteCoordinator) Proxy.newProxyInstance(DubboRemoteCoordinator.class.getClassLoader(),
					new Class[] { RemoteCoordinator.class }, dubboCoordinator);

			coordinatorRegistry.putApplication(remoteAddr, targetName);
			coordinatorRegistry.putRemoteAddr(instanceId, remoteAddr);

			coordinatorRegistry.putRemoteCoordinator(instanceId, remoteCoordinator);
			coordinatorRegistry.putRemoteCoordinatorByAddr(remoteAddr, remoteCoordinator);
		}

		TransactionRequestImpl request = new TransactionRequestImpl();
		request.setTargetTransactionCoordinator(remoteCoordinator);

		TransactionResponseImpl response = new TransactionResponseImpl();
		response.setSourceTransactionCoordinator(remoteCoordinator);

		String propagatedBy = null;
		boolean failure = false;
		try {
			this.beforeProviderInvokeForSVC(invocation, request, response);

			Transaction transaction = transactionManager.getTransactionQuietly();
			TransactionContext transactionContext = transaction == null ? null : transaction.getTransactionContext();
			propagatedBy = transactionContext == null ? null : String.valueOf(transactionContext.getPropagatedBy());

			return this.wrapResultForProvider(invoker, invocation, propagatedBy, true);
		} catch (RpcException rex) {
			failure = true;

			return this.createErrorResultForProvider(rex, propagatedBy, true);
		} catch (Throwable rex) {
			failure = true;
			logger.error("Error occurred in remote call!", rex);

			return this.createErrorResultForProvider(rex, propagatedBy, true);
		} finally {
			try {
				this.afterProviderInvokeForSVC(invocation, request, response);
			} catch (RpcException rex) {
				if (failure) {
					logger.error("Error occurred in remote call!", rex);
				} else {
					return this.createErrorResultForProvider(rex, propagatedBy, true);
				}
			} catch (Throwable rex) {
				if (failure) {
					logger.error("Error occurred in remote call!", rex);
				} else {
					return this.createErrorResultForProvider(rex, propagatedBy, true);
				}
			}
		}

	}

	public Result wrapResultForProvider(Invoker<?> invoker, Invocation invocation, String propagatedBy,
			boolean attachRequired) {

		try {
			RpcResult result = (RpcResult) invoker.invoke(invocation);
			if (result.hasException()) {
				return this.createErrorResultForProvider(result.getException(), propagatedBy, attachRequired);
			} else {
				return this.convertResultForProvider(result, propagatedBy, attachRequired);
			}
		} catch (Throwable rex) {
			return this.createErrorResultForProvider(rex, propagatedBy, attachRequired);
		}

	}

	private Result convertResultForProvider(RpcResult result, String propagatedBy, boolean attachRequired) {
		TransactionBeanRegistry beanRegistry = TransactionBeanRegistry.getInstance();
		TransactionBeanFactory beanFactory = beanRegistry.getBeanFactory();
		RemoteCoordinator transactionCoordinator = beanFactory.getTransactionCoordinator();

		Object value = result.getValue();

		InvocationResult wrapped = new InvocationResult();
		wrapped.setValue(value);
		if (attachRequired) {
			wrapped.setVariable(Propagation.class.getName(), propagatedBy);
			wrapped.setVariable(RemoteCoordinator.class.getName(), transactionCoordinator.getIdentifier());
		}

		result.setException(null);
		result.setValue(wrapped);

		return result;
	}

	private Result createErrorResultForProvider(Throwable throwable, String propagatedBy, boolean attachRequired) {
		TransactionBeanRegistry beanRegistry = TransactionBeanRegistry.getInstance();
		TransactionBeanFactory beanFactory = beanRegistry.getBeanFactory();
		RemoteCoordinator transactionCoordinator = beanFactory.getTransactionCoordinator();

		RpcResult result = new RpcResult();

		InvocationResult wrapped = new InvocationResult();
		wrapped.setError(throwable);
		if (attachRequired) {
			wrapped.setVariable(Propagation.class.getName(), propagatedBy);
			wrapped.setVariable(RemoteCoordinator.class.getName(), transactionCoordinator.getIdentifier());
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
		String propagatedBy = invocation.getAttachment(RemoteCoordinator.class.getName());
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
		if (StringUtils.equals(invocation.getMethodName(), KEY_XA_RESOURCE_START)
				&& Arrays.equals(invocation.getParameterTypes(), new Class<?>[] { Xid.class, Integer.TYPE })) {
			return this.consumerInvokeForKey(invoker, invocation);
		} else if (XAResource.class.getName().equals(interfaceClazz)) {
			return this.consumerInvokeForJTA(invoker, invocation);
		} else if (RemoteCoordinator.class.getName().equals(interfaceClazz)) {
			return this.consumerInvokeForJTA(invoker, invocation);
		} else {
			return this.consumerInvokeForSVC(invoker, invocation);
		}
	}

	public Result consumerInvokeForKey(Invoker<?> invoker, Invocation invocation) throws RpcException {
		RemoteCoordinatorRegistry coordinatorRegistry = RemoteCoordinatorRegistry.getInstance();
		TransactionBeanRegistry beanRegistry = TransactionBeanRegistry.getInstance();
		TransactionBeanFactory beanFactory = beanRegistry.getBeanFactory();
		RemoteCoordinator transactionCoordinator = beanFactory.getTransactionCoordinator();
		RemoteCoordinator consumeCoordinator = beanRegistry.getConsumeCoordinator();

		String instanceId = null;

		Map<String, String> attachments = invocation.getAttachments();
		attachments.put(RemoteCoordinator.class.getName(), transactionCoordinator.getIdentifier());

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

			instanceId = StringUtils.trimToEmpty(String.valueOf(wrapped.getVariable(RemoteCoordinator.class.getName())));
		} // end-if (InvocationResult.class.isInstance(value))

		if (StringUtils.isNotBlank(instanceId) && coordinatorRegistry.getRemoteCoordinator(instanceId) == null) {
			String[] values = instanceId == null ? new String[0] : instanceId.split("\\s*:\\s*");

			String targetAddr = values.length == 3 ? values[0] : StringUtils.EMPTY;
			String targetName = values.length == 3 ? values[1] : StringUtils.EMPTY;
			String targetPort = values.length == 3 ? values[2] : String.valueOf(0);

			String remoteAddr = StringUtils.isBlank(targetAddr) && StringUtils.isBlank(targetPort) //
					? StringUtils.EMPTY : String.format("%s:%s", targetAddr, targetPort);

			coordinatorRegistry.putApplication(remoteAddr, targetName);
			coordinatorRegistry.putRemoteAddr(instanceId, remoteAddr);

			RemoteCoordinator remoteCoordinator = coordinatorRegistry.getRemoteCoordinatorByAddr(remoteAddr);
			if (remoteCoordinator == null) {
				InvocationContext invocationContext = new InvocationContext();
				invocationContext.setServerHost(targetAddr);
				invocationContext.setServiceKey(targetName);
				invocationContext.setServerPort(Integer.valueOf(targetPort));

				DubboRemoteCoordinator dubboCoordinator = new DubboRemoteCoordinator();
				dubboCoordinator.setInvocationContext(invocationContext);
				dubboCoordinator.setRemoteCoordinator(consumeCoordinator);

				remoteCoordinator = (RemoteCoordinator) Proxy.newProxyInstance(DubboRemoteCoordinator.class.getClassLoader(),
						new Class[] { RemoteCoordinator.class }, dubboCoordinator);

				coordinatorRegistry.putRemoteCoordinatorByAddr(remoteAddr, remoteCoordinator);
				coordinatorRegistry.putRemoteCoordinator(instanceId, remoteCoordinator);
			} else {
				DubboRemoteCoordinator dubboCoordinator = (DubboRemoteCoordinator) Proxy
						.getInvocationHandler(remoteCoordinator);
				dubboCoordinator.getInvocationContext().setServiceKey(targetName);

				// coordinatorRegistry.putRemoteCoordinatorByAddr(remoteAddr, remoteCoordinator);
				coordinatorRegistry.putRemoteCoordinator(instanceId, remoteCoordinator);
			}
		}

		return result;
	}

	public Result consumerInvokeForJTA(Invoker<?> invoker, Invocation invocation) throws RpcException {
		TransactionBeanRegistry beanRegistry = TransactionBeanRegistry.getInstance();
		TransactionBeanFactory beanFactory = beanRegistry.getBeanFactory();
		RemoteCoordinator transactionCoordinator = beanFactory.getTransactionCoordinator();

		Map<String, String> attachments = invocation.getAttachments();
		attachments.put(RemoteCoordinator.class.getName(), transactionCoordinator.getIdentifier());
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

		} // end-if (InvocationResult.class.isInstance(value))

		return result;
	}

	public Result consumerInvokeForSVC(Invoker<?> invoker, Invocation invocation) throws RpcException {
		RemoteCoordinatorRegistry coordinatorRegistry = RemoteCoordinatorRegistry.getInstance();
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
		String remoteAddr = String.format("%s:%s", targetAddr, targetPort);

		String targetName = coordinatorRegistry.getApplication(remoteAddr);
		String instanceId = String.format("%s:%s:%s", targetAddr, targetName, targetPort);

		InvocationContext invocationContext = new InvocationContext();
		invocationContext.setServerHost(targetAddr);
		invocationContext.setServiceKey(targetName);
		invocationContext.setServerPort(targetPort);

		RemoteCoordinator remoteCoordinator = StringUtils.isNotBlank(targetName)
				? coordinatorRegistry.getRemoteCoordinator(instanceId)
				: coordinatorRegistry.getRemoteCoordinatorByAddr(remoteAddr);
		if (remoteCoordinator == null) {
			DubboRemoteCoordinator dubboCoordinator = new DubboRemoteCoordinator();
			dubboCoordinator.setInvocationContext(invocationContext);
			dubboCoordinator.setRemoteCoordinator(consumeCoordinator);

			remoteCoordinator = (RemoteCoordinator) Proxy.newProxyInstance(DubboRemoteCoordinator.class.getClassLoader(),
					new Class[] { RemoteCoordinator.class }, dubboCoordinator);
			coordinatorRegistry.putRemoteCoordinatorByAddr(remoteAddr, remoteCoordinator);
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

				String propagatedBy = (String) wrapped.getVariable(Propagation.class.getName());
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
				} else {
					logger.error("Error occurred in remote call!", rex);
				}
			} catch (RuntimeException rex) {
				if (success) {
					RpcResult result = new RpcResult();
					result.setException(new RpcException("Error occurred in remote call!", rex));
					return result;
				} else {
					logger.error("Error occurred in remote call!", rex);
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

		Map<String, String> attachments = invocation.getAttachments();
		attachments.put(RemoteCoordinator.class.getName(), transactionCoordinator.getIdentifier());

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
			attachments.put(TransactionContext.class.getName(), transactionContextContent);
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
