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
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.io.IOUtils;
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
		RemoteNode remoteNode = CommonUtils.getRemoteNode(identifier);
		if (remoteNode == null) {
			return null;
		} else {
			return String.format("%s:%s", remoteNode.getServerHost(), remoteNode.getServerPort());
		}
	}

	public static boolean applicationEquals(String source, String target) {
		String sourceApplication = CommonUtils.getApplication(source);
		String targetApplication = CommonUtils.getApplication(target);
		if (StringUtils.isBlank(sourceApplication) || StringUtils.isBlank(targetApplication)) {
			return false;
		} else {
			return StringUtils.equalsIgnoreCase(sourceApplication, targetApplication);
		}
	}

	public static boolean instanceKeyEquals(String source, String target) {
		RemoteAddr sourceAddr = CommonUtils.getRemoteAddr(source);
		RemoteAddr targetAddr = CommonUtils.getRemoteAddr(target);
		if (sourceAddr == null || targetAddr == null) {
			return false;
		} else {
			String sourceHost = sourceAddr.getServerHost();
			String targetHost = targetAddr.getServerHost();
			int sourcePort = sourceAddr.getServerPort();
			int targetPort = targetAddr.getServerPort();
			return StringUtils.equalsIgnoreCase(sourceHost, targetHost) && sourcePort == targetPort;
		}
	}

	public static boolean equals(Object o1, Object o2) {
		return java.util.Objects.equals(o1, o2);
	}

	public static void closeQuietly(Closeable closeable) {
		IOUtils.closeQuietly(closeable);
	}

	public static String getInetAddress() {
		Enumeration<NetworkInterface> interfaces = null;
		try {
			interfaces = NetworkInterface.getNetworkInterfaces();
		} catch (SocketException ignored) {
			return CommonUtils.getLocalHostAddress();
		}

		final Set<InetAddress> virtualist = new TreeSet<InetAddress>(new InetAddrComparator());
		final Set<InetAddress> candidates = new TreeSet<InetAddress>(new InetAddrComparator());
		while (interfaces != null && interfaces.hasMoreElements()) {
			NetworkInterface network = interfaces.nextElement();
			try {
				if (network.isUp() == false || network.isLoopback() || network.isPointToPoint()) {
					continue;
				}
			} catch (SocketException ignored) {
				continue;
			}

			Enumeration<InetAddress> addresses = network.getInetAddresses();
			while (addresses.hasMoreElements()) {
				InetAddress address = addresses.nextElement();
				if (Inet4Address.class.isInstance(address) == false) {
					continue;
				} else if (address.isAnyLocalAddress() || address.isMulticastAddress()) {
					continue;
				}

				(network.isVirtual() ? virtualist : candidates).add(address);
			}
		}

		Iterator<InetAddress> itr = candidates.iterator();
		Iterator<InetAddress> vtr = virtualist.iterator();
		if (itr.hasNext()) {
			return itr.next().getHostAddress();
		} else if (vtr.hasNext()) {
			return vtr.next().getHostAddress();
		}

		return CommonUtils.getLocalHostAddress();
	}

	private static class InetAddrComparator implements Comparator<InetAddress> {
		public int compare(InetAddress o1, InetAddress o2) {
			boolean linkLocalAddr1 = o1.isLinkLocalAddress();
			boolean siteLocalAddr1 = o1.isSiteLocalAddress();
			boolean linkLocalAddr2 = o2.isLinkLocalAddress();
			boolean siteLocalAddr2 = o2.isSiteLocalAddress();
			boolean linkLocalEquals = linkLocalAddr1 == linkLocalAddr2;
			boolean siteLocalEquals = siteLocalAddr1 == siteLocalAddr2;
			if (linkLocalEquals && siteLocalEquals) {
				return -1;
			} else if (linkLocalEquals == false && siteLocalEquals == false) {
				return linkLocalAddr1 ? -1 : 1;
			} else if (siteLocalEquals) {
				return linkLocalAddr1 ? -1 : 1;
			} else if (linkLocalEquals) {
				return siteLocalAddr1 ? -1 : 1;
			} else {
				return -1;
			}
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

	public static String getLocalHostAddress() {
		try {
			InetAddress inetAddr = InetAddress.getLocalHost();
			return inetAddr.getHostAddress();
		} catch (Exception ex) {
			logger.error("Error occurred while getting ip address.", ex);
			return "127.0.0.1";
		}
	}

}
