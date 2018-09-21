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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.bytesoft.transaction.remote.RemoteAddr;
import org.bytesoft.transaction.remote.RemoteCoordinator;
import org.bytesoft.transaction.remote.RemoteNode;

public class RemoteCoordinatorRegistry {
	static final RemoteCoordinatorRegistry instance = new RemoteCoordinatorRegistry();

	private final Map<RemoteAddr, RemoteNode> instances = new ConcurrentHashMap<RemoteAddr, RemoteNode>();
	private final Map<String, RemoteCoordinator> participants = new ConcurrentHashMap<String, RemoteCoordinator>();

	public void putRemoteNode(RemoteAddr remoteAddr, RemoteNode remoteNode) {
		this.instances.put(remoteAddr, remoteNode);
	}

	public RemoteNode getRemoteNode(RemoteAddr remoteAddr) {
		return this.instances.get(remoteAddr);
	}

	public boolean containsRemoteNode(RemoteAddr remoteAddr) {
		return this.instances.containsKey(remoteAddr);
	}

	public void removeRemoteNode(RemoteAddr remoteAddr) {
		this.instances.remove(remoteAddr);
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

}
