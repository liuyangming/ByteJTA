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
package org.bytesoft.bytejta.supports.internal;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.StringUtils;
import org.bytesoft.common.utils.CommonUtils;
import org.bytesoft.transaction.remote.RemoteAddr;
import org.bytesoft.transaction.remote.RemoteCoordinator;
import org.bytesoft.transaction.remote.RemoteNode;

public class RemoteCoordinatorRegistry {
	static final RemoteCoordinatorRegistry instance = new RemoteCoordinatorRegistry();

	/* host:port -> participant */
	private final Map<RemoteAddr, RemoteCoordinator> instances = new ConcurrentHashMap<RemoteAddr, RemoteCoordinator>();
	/* host:port -> host:application:port */
	private final Map<RemoteAddr, RemoteNode> mappings = new ConcurrentHashMap<RemoteAddr, RemoteNode>();
	/* application -> participant */
	private final Map<String, RemoteCoordinator> participants = new ConcurrentHashMap<String, RemoteCoordinator>();

	/* invocation -> application */
	private final Map<InvocationDefinition, String> applications = new ConcurrentHashMap<InvocationDefinition, String>();

	public void putInvocationDef(InvocationDefinition invocationDef, String application) {
		this.applications.put(invocationDef, application);
	}

	public String getInvocationDef(InvocationDefinition invocationDef) {
		return this.applications.get(invocationDef);
	}

	public boolean containsInvocationDef(InvocationDefinition invocationDef) {
		return this.applications.containsKey(invocationDef);
	}

	public void removetInvocationDef(InvocationDefinition invocationDef) {
		this.applications.remove(invocationDef);
	}

	public void putInstance(RemoteAddr remoteAddr, RemoteCoordinator participant) {
		this.instances.put(remoteAddr, participant);
	}

	public RemoteCoordinator getInstance(RemoteAddr remoteAddr) {
		return this.instances.get(remoteAddr);
	}

	public boolean containsInstance(RemoteAddr remoteAddr) {
		return this.instances.containsKey(remoteAddr);
	}

	public void removetInstance(RemoteAddr remoteAddr) {
		this.instances.remove(remoteAddr);
	}

	public void putRemoteNode(RemoteAddr remoteAddr, RemoteNode remoteNode) {
		this.mappings.put(remoteAddr, remoteNode);
	}

	public RemoteNode getRemoteNode(RemoteAddr remoteAddr) {
		return this.mappings.get(remoteAddr);
	}

	public boolean containsRemoteNode(RemoteAddr remoteAddr) {
		return this.mappings.containsKey(remoteAddr);
	}

	public void removeRemoteNode(RemoteAddr remoteAddr) {
		this.mappings.remove(remoteAddr);
	}

	public void putParticipant(String application, RemoteCoordinator participant) {
		this.participants.put(application, participant);
	}

	public boolean containsParticipant(String application) {
		return this.participants.containsKey(application);
	}

	public RemoteCoordinator getParticipant(String application) {
		return this.participants.get(application);
	}

	public void removeParticipant(String application) {
		this.participants.remove(application);
	}

	private RemoteCoordinatorRegistry() {
		if (instance != null) {
			throw new IllegalStateException();
		}
	}

	public static RemoteCoordinatorRegistry getInstance() {
		return instance;
	}

	public static class InvocationDefinition {
		private Class<?> interfaceClass;
		private String methodName;
		private Class<?>[] parameterTypes;

		public int hashCode() {
			int hash = 3;
			hash += 5 * this.interfaceClass.hashCode();
			hash += 7 * this.methodName.hashCode();
			hash += 11 * Arrays.hashCode(this.parameterTypes);
			return hash;
		}

		public boolean equals(Object obj) {
			if (obj == null) {
				return false;
			} else if (InvocationDefinition.class.isInstance(obj) == false) {
				return false;
			}
			InvocationDefinition that = (InvocationDefinition) obj;
			boolean clazzEquals = CommonUtils.equals(this.interfaceClass, that.interfaceClass);
			boolean methodEquals = StringUtils.equals(this.methodName, that.methodName);
			boolean typesEquals = Arrays.equals(this.parameterTypes, that.parameterTypes);
			return clazzEquals && methodEquals && typesEquals;
		}

		public Class<?> getInterfaceClass() {
			return interfaceClass;
		}

		public void setInterfaceClass(Class<?> interfaceClass) {
			this.interfaceClass = interfaceClass;
		}

		public String getMethodName() {
			return methodName;
		}

		public void setMethodName(String methodName) {
			this.methodName = methodName;
		}

		public Class<?>[] getParameterTypes() {
			return parameterTypes;
		}

		public void setParameterTypes(Class<?>[] parameterTypes) {
			this.parameterTypes = parameterTypes;
		}
	}

}
