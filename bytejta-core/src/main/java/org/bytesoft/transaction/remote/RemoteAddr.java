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
package org.bytesoft.transaction.remote;

import java.io.Serializable;

import org.apache.commons.lang3.StringUtils;

public class RemoteAddr implements Serializable {
	private static final long serialVersionUID = 1L;

	protected String serverHost;
	protected int serverPort;

	public int hashCode() {
		int hash = 7;
		hash += 11 * (this.serverHost == null ? 0 : this.serverHost.hashCode());
		hash += 13 * this.serverPort;
		return hash;
	}

	public boolean equals(Object obj) {
		if (obj == null) {
			return false;
		} else if (RemoteAddr.class.isInstance(obj) == false) {
			return false;
		}
		RemoteAddr that = (RemoteAddr) obj;
		boolean hostEquals = StringUtils.equals(this.serverHost, that.serverHost);
		boolean portEquals = this.serverPort == that.serverPort;
		return hostEquals && portEquals;
	}

	public String toString() {
		return String.format("<remote-addr| %s:%s>", this.serverHost, this.serverPort);
	}

	public String getServerHost() {
		return serverHost;
	}

	public void setServerHost(String serverHost) {
		this.serverHost = serverHost;
	}

	public int getServerPort() {
		return serverPort;
	}

	public void setServerPort(int serverPort) {
		this.serverPort = serverPort;
	}
}
