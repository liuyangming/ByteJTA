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
package org.bytesoft.common.utils;

import java.io.Closeable;
import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.UnknownHostException;
import java.util.Enumeration;

import org.apache.commons.lang3.StringUtils;
import org.bytesoft.transaction.remote.RemoteAddr;
import org.bytesoft.transaction.remote.RemoteNode;
import org.bytesoft.transaction.remote.RemoteSvc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommonUtils {
	static final Logger logger = LoggerFactory.getLogger(CommonUtils.class);

	public static RemoteAddr getRemoteAddr(String identifier) {
		if (StringUtils.isBlank(identifier)) {
			return null;
		} else {
			String[] values = identifier.split("\\s*:\\s*");
			if (values.length != 3) {
				return null;
			}

			RemoteAddr remoteAddr = new RemoteAddr();
			remoteAddr.setServerHost(values[0]);
			remoteAddr.setServerPort(Integer.parseInt(values[2]));
			return remoteAddr;
		}
	}

	public static RemoteNode getRemoteNode(String identifier) {
		if (StringUtils.isBlank(identifier)) {
			return null;
		} else {
			String[] values = identifier.split("\\s*:\\s*");
			if (values.length != 3) {
				return null;
			}

			RemoteNode remoteNode = new RemoteNode();
			remoteNode.setServerHost(values[0]);
			remoteNode.setServiceKey(values[1]);
			remoteNode.setServerPort(Integer.parseInt(values[2]));
			return remoteNode;
		}
	}

	public static RemoteSvc getRemoteSvc(RemoteNode remoteNode) {
		RemoteSvc remoteSvc = new RemoteSvc();
		remoteSvc.setServerHost(remoteNode.getServerHost());
		remoteSvc.setServiceKey(remoteNode.getServiceKey());
		remoteSvc.setServerPort(remoteNode.getServerPort());
		return remoteSvc;
	}

	public static RemoteSvc getRemoteSvc(String identifier) {
		RemoteNode remoteNode = getRemoteNode(identifier);
		return getRemoteSvc(remoteNode);
	}

	public static String getApplication(String identifier) {
		if (StringUtils.isBlank(identifier)) {
			return null;
		} else {
			String[] values = identifier.split("\\s*:\\s*");
			return values.length == 3 ? values[1] : null;
		}
	}

	public static String getInstanceKey(String identifier) {
		if (StringUtils.isBlank(identifier)) {
			return null;
		} else {
			String[] values = identifier.split("\\s*:\\s*");
			return values.length == 3 ? values[1] : String.format("%s:%s", values[0], values[2]);
		}
	}

	public static boolean applicationEquals(String source, String target) {
		if (StringUtils.isBlank(source) && StringUtils.isNotBlank(target)) {
			return false;
		} else if (StringUtils.isBlank(target) && StringUtils.isNotBlank(source)) {
			return false;
		} else if (StringUtils.isBlank(target) && StringUtils.isBlank(source)) {
			return false; // treat as different nodes.
		} else {
			String[] sourceArray = source.split("\\s*:\\s*");
			String[] targetArray = target.split("\\s*:\\s*");
			if (sourceArray.length != targetArray.length) {
				return false;
			} else if (sourceArray.length != 3) {
				return false;
			} else {
				String sourceName = sourceArray[1];
				String targetName = targetArray[1];
				return StringUtils.equalsIgnoreCase(sourceName, targetName);
			}
		}
	}

	public static boolean instanceKeyEquals(String source, String target) {
		if (StringUtils.isBlank(source) && StringUtils.isNotBlank(target)) {
			return false;
		} else if (StringUtils.isBlank(target) && StringUtils.isNotBlank(source)) {
			return false;
		} else if (StringUtils.isBlank(target) && StringUtils.isBlank(source)) {
			return false; // treat as different nodes.
		} else {
			String[] sourceArray = source.split("\\s*:\\s*");
			String[] targetArray = target.split("\\s*:\\s*");
			if (sourceArray.length != targetArray.length) {
				return false;
			} else {
				String sourceAddr = sourceArray[0];
				String targetAddr = targetArray[0];
				String sourcePort = sourceArray[sourceArray.length - 1];
				String targetPort = targetArray[targetArray.length - 1];
				return StringUtils.equalsIgnoreCase(sourceAddr, targetAddr)
						&& StringUtils.equalsIgnoreCase(sourcePort, targetPort);
			}
		}
	}

	public static boolean equals(Object o1, Object o2) {
		if (o1 != null) {
			return o1.equals(o2);
		} else if (o2 != null) {
			return o2.equals(o1);
		} else {
			return true;
		}
	}

	public static void closeQuietly(Closeable closeable) {
		if (closeable != null) {
			try {
				closeable.close();
			} catch (IOException ex) {
				logger.debug("Error occurred while closing resource {}.", closeable);
			}
		}
	}

	public static String getInetAddress() {
		try {
			Enumeration<NetworkInterface> enumeration = NetworkInterface.getNetworkInterfaces();
			while (enumeration.hasMoreElements()) {
				NetworkInterface ni = enumeration.nextElement();
				if (ni.isLoopback()) {
					continue;
				} else if (ni.isUp() == false) {
					continue;
				}

				Enumeration<InetAddress> inetAddrList = ni.getInetAddresses();
				while (inetAddrList.hasMoreElements()) {
					InetAddress inetAddr = inetAddrList.nextElement();

					if (inetAddr.isLoopbackAddress()) {
						continue;
					} else if (inetAddr.isMulticastAddress()) {
						continue;
					} else if (inetAddr.isAnyLocalAddress()) {
						continue;
					} else if (Inet4Address.class.isInstance(inetAddr) == false) {
						continue;
					}

					return inetAddr.getHostAddress();
				}

			}

			InetAddress inetAddr = InetAddress.getLocalHost();
			return inetAddr.getHostAddress();
		} catch (Exception ex) {
			logger.error("Error occurred while getting ip address.", ex);
			return "127.0.0.1";
		}
	}

	public static String getInetAddress(String host) {
		try {
			InetAddress inetAddr = InetAddress.getByName(host);
			return inetAddr.getHostAddress();
		} catch (UnknownHostException ex) {
			logger.error("Error occurred while getting ip address: host= {}.", host, ex);
			return host;
		}
	}

}
