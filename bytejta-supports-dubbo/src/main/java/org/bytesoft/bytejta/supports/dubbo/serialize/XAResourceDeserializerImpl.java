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
package org.bytesoft.bytejta.supports.dubbo.serialize;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.bytesoft.bytejta.supports.dubbo.TransactionBeanRegistry;
import org.bytesoft.bytejta.supports.internal.RemoteCoordinatorRegistry;
import org.bytesoft.bytejta.supports.resource.RemoteResourceDescriptor;
import org.bytesoft.common.utils.CommonUtils;
import org.bytesoft.transaction.remote.RemoteAddr;
import org.bytesoft.transaction.remote.RemoteCoordinator;
import org.bytesoft.transaction.remote.RemoteNode;
import org.bytesoft.transaction.supports.resource.XAResourceDescriptor;
import org.bytesoft.transaction.supports.serialize.XAResourceDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.config.ApplicationConfig;
import com.alibaba.dubbo.config.ProtocolConfig;
import com.alibaba.dubbo.config.ReferenceConfig;
import com.alibaba.dubbo.config.RegistryConfig;
import com.alibaba.dubbo.rpc.RpcException;

public class XAResourceDeserializerImpl implements XAResourceDeserializer, ApplicationContextAware {
	static final Logger logger = LoggerFactory.getLogger(XAResourceDeserializerImpl.class);
	static Pattern pattern = Pattern.compile("^[^:]+\\s*:\\s*[^:]+\\s*:\\s*\\d+$");

	private XAResourceDeserializer resourceDeserializer;
	private ApplicationContext applicationContext;

	public XAResourceDescriptor deserialize(String identifier) {
		XAResourceDescriptor resourceDescriptor = this.resourceDeserializer.deserialize(identifier);
		if (resourceDescriptor != null) {
			return resourceDescriptor;
		}

		Matcher matcher = pattern.matcher(identifier);
		if (matcher.find()) {
			RemoteCoordinatorRegistry registry = RemoteCoordinatorRegistry.getInstance();
			RemoteAddr remoteAddr = CommonUtils.getRemoteAddr(identifier);
			RemoteNode remoteNode = CommonUtils.getRemoteNode(identifier);

			RemoteNode targetNode = registry.getRemoteNode(remoteAddr);
			if (targetNode == null && remoteAddr != null && remoteNode != null) {
				registry.putRemoteNode(remoteAddr, remoteNode);
			}

			this.initializePhysicalInstanceIfNecessary(remoteAddr);

			String application = CommonUtils.getApplication(identifier);
			this.initializeRemoteParticipantIfNecessary(application);

			RemoteResourceDescriptor descriptor = new RemoteResourceDescriptor();
			descriptor.setIdentifier(identifier);
			descriptor.setDelegate(registry.getPhysicalInstance(remoteAddr));
			// descriptor.setDelegate(registry.getParticipant(application));

			return descriptor;
		} else {
			logger.error("can not find a matching xa-resource(identifier= {})!", identifier);
			return null;
		}

	}

	private void initializePhysicalInstanceIfNecessary(RemoteAddr remoteAddr) throws RpcException {
		RemoteCoordinatorRegistry participantRegistry = RemoteCoordinatorRegistry.getInstance();
		RemoteCoordinator physicalInst = participantRegistry.getPhysicalInstance(remoteAddr);
		if (physicalInst == null) {
			String serverHost = remoteAddr.getServerHost();
			int serverPort = remoteAddr.getServerPort();
			final String target = String.format("%s:%s", serverHost, serverPort).intern();
			synchronized (target) {
				RemoteCoordinator participant = participantRegistry.getPhysicalInstance(remoteAddr);
				if (participant == null) {
					this.processInitPhysicalInstanceIfNecessary(remoteAddr);
				}
			} // end-synchronized (target)
		} // end-if (physicalInst == null)
	}

	private void processInitPhysicalInstanceIfNecessary(RemoteAddr remoteAddr) throws RpcException {
		RemoteCoordinatorRegistry participantRegistry = RemoteCoordinatorRegistry.getInstance();
		TransactionBeanRegistry beanRegistry = TransactionBeanRegistry.getInstance();

		RemoteCoordinator participant = participantRegistry.getPhysicalInstance(remoteAddr);
		if (participant == null) {
			ApplicationConfig applicationConfig = beanRegistry.getBean(ApplicationConfig.class);
			RegistryConfig registryConfig = beanRegistry.getBean(RegistryConfig.class);
			ProtocolConfig protocolConfig = beanRegistry.getBean(ProtocolConfig.class);

			ReferenceConfig<RemoteCoordinator> referenceConfig = new ReferenceConfig<RemoteCoordinator>();
			referenceConfig.setInterface(RemoteCoordinator.class);
			referenceConfig.setTimeout(15000);
			referenceConfig.setCluster("failfast");
			referenceConfig.setLoadbalance("bytejta");
			referenceConfig.setFilter("bytejta");
			referenceConfig.setGroup("org-bytesoft-bytejta");
			referenceConfig.setCheck(false);
			referenceConfig.setRetries(-1);
			referenceConfig.setUrl(String.format("%s:%s", remoteAddr.getServerHost(), remoteAddr.getServerPort()));
			referenceConfig.setScope(Constants.SCOPE_REMOTE);

			referenceConfig.setApplication(applicationConfig);
			if (registryConfig != null) {
				referenceConfig.setRegistry(registryConfig);
			}
			if (protocolConfig != null) {
				referenceConfig.setProtocol(protocolConfig.getName());
			} // end-if (protocolConfig != null)

			RemoteCoordinator reference = referenceConfig.get();
			if (reference == null) {
				throw new RpcException("Cannot get the application name of the remote application.");
			}

			participantRegistry.putPhysicalInstance(remoteAddr, reference);
		}
	}

	private void initializeRemoteParticipantIfNecessary(final String system) throws RpcException {
		RemoteCoordinatorRegistry participantRegistry = RemoteCoordinatorRegistry.getInstance();
		final String application = StringUtils.trimToEmpty(system).intern();
		RemoteCoordinator remoteParticipant = participantRegistry.getParticipant(application);
		if (remoteParticipant == null) {
			synchronized (application) {
				RemoteCoordinator participant = participantRegistry.getParticipant(application);
				if (participant == null) {
					this.processInitRemoteParticipantIfNecessary(application);
				}
			} // end-synchronized (target)
		} // end-if (remoteParticipant == null)
	}

	private void processInitRemoteParticipantIfNecessary(String application) {
		RemoteCoordinatorRegistry participantRegistry = RemoteCoordinatorRegistry.getInstance();
		TransactionBeanRegistry beanRegistry = TransactionBeanRegistry.getInstance();

		RemoteCoordinator participant = participantRegistry.getParticipant(application);
		if (participant == null) {
			ApplicationConfig applicationConfig = beanRegistry.getBean(ApplicationConfig.class);
			RegistryConfig registryConfig = beanRegistry.getBean(RegistryConfig.class);
			ProtocolConfig protocolConfig = beanRegistry.getBean(ProtocolConfig.class);

			ReferenceConfig<RemoteCoordinator> referenceConfig = new ReferenceConfig<RemoteCoordinator>();
			referenceConfig.setInterface(RemoteCoordinator.class);
			referenceConfig.setTimeout(15000);
			referenceConfig.setCluster("failfast");
			referenceConfig.setLoadbalance("bytejta");
			referenceConfig.setFilter("bytejta");
			referenceConfig.setGroup(application);
			referenceConfig.setCheck(false);
			referenceConfig.setRetries(-1);
			referenceConfig.setScope(Constants.SCOPE_REMOTE);

			referenceConfig.setApplication(applicationConfig);
			if (registryConfig != null) {
				referenceConfig.setRegistry(registryConfig);
			}
			if (protocolConfig != null) {
				referenceConfig.setProtocol(protocolConfig.getName());
			} // end-if (protocolConfig != null)

			RemoteCoordinator reference = referenceConfig.get();
			if (reference == null) {
				throw new RpcException("Cannot get the application name of the remote application.");
			}

			participantRegistry.putParticipant(application, reference);
		}
	}

	public XAResourceDeserializer getResourceDeserializer() {
		return resourceDeserializer;
	}

	public void setResourceDeserializer(XAResourceDeserializer resourceDeserializer) {
		this.resourceDeserializer = resourceDeserializer;
	}

	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}

	public ApplicationContext getApplicationContext() {
		return applicationContext;
	}

}
