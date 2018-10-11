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

import org.apache.commons.lang3.StringUtils;

public class RemoteSvc extends RemoteAddr {
	private static final long serialVersionUID = 1L;

	protected String serviceKey;

	public int hashCode() {
		int hash = 19;
		hash += 23 * (this.serviceKey == null ? 0 : this.serviceKey.hashCode());
		return hash;
	}

	public boolean equals(Object obj) {
		if (obj == null) {
			return false;
		} else if (RemoteSvc.class.isInstance(obj) == false) {
			return false;
		}
		RemoteSvc that = (RemoteSvc) obj;
		return StringUtils.equals(this.serviceKey, that.serviceKey);
	}

	public String toString() {
		return String.format("<remote-svc| serviceKey= %s>", this.serviceKey);
	}

	public String getServiceKey() {
		return serviceKey;
	}

	public void setServiceKey(String serviceKey) {
		this.serviceKey = serviceKey;
	}
}
