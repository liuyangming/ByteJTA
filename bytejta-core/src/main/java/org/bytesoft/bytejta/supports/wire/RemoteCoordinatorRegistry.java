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
package org.bytesoft.bytejta.supports.wire;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RemoteCoordinatorRegistry {
	static final RemoteCoordinatorRegistry instance = new RemoteCoordinatorRegistry();

	private final Map<String, String> instanceKeysMap = new ConcurrentHashMap<String, String>();
	private final Map<String, String> inetAddr2AppMap = new ConcurrentHashMap<String, String>();

	private final Map<String, RemoteCoordinator> remoteAddrMap = new ConcurrentHashMap<String, RemoteCoordinator>();
	private final Map<String, RemoteCoordinator> instanceIdMap = new ConcurrentHashMap<String, RemoteCoordinator>();

	public void putRemoteAddr(String instanceId, String remoteAddr) {
		this.instanceKeysMap.put(instanceId, remoteAddr);
	}

	public boolean containsRemoteAddr(String instanceId) {
		return this.instanceKeysMap.containsKey(instanceId);
	}

	public String getRemoteAddr(String instanceId) {
		return this.instanceKeysMap.get(instanceId);
	}

	public void removeRemoteAddr(String instanceId) {
		this.instanceIdMap.remove(instanceId);
	}

	public void putApplication(String remoteAddr, String application) {
		this.inetAddr2AppMap.put(remoteAddr, application);
	}

	public boolean containsApplication(String remoteAddr) {
		return this.inetAddr2AppMap.containsKey(remoteAddr);
	}

	public String getApplication(String remoteAddr) {
		return this.inetAddr2AppMap.get(remoteAddr);
	}

	public void remoteApplication(String remoteAddr) {
		this.inetAddr2AppMap.remove(remoteAddr);
	}

	public void putRemoteCoordinatorByAddr(String remoteAddr, RemoteCoordinator stub) {
		this.remoteAddrMap.put(remoteAddr, stub);
	}

	public RemoteCoordinator getRemoteCoordinatorByAddr(String remoteAddr) {
		return this.remoteAddrMap.get(remoteAddr);
	}

	public void removeRemoteCoordinatorByAddr(String remoteAddr) {
		this.remoteAddrMap.remove(remoteAddr);
	}

	public void putRemoteCoordinator(String instanceId, RemoteCoordinator stub) {
		this.instanceIdMap.put(instanceId, stub);
	}

	public RemoteCoordinator getRemoteCoordinator(String instanceId) {
		return this.instanceIdMap.get(instanceId);
	}

	public void removeRemoteCoordinator(String instanceId) {
		this.instanceIdMap.remove(instanceId);
	}

	private RemoteCoordinatorRegistry() {
		if (instance != null) {
			throw new IllegalStateException();
		}
	}

	public static RemoteCoordinatorRegistry getInstance() {
		return instance;
	}

}
