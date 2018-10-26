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
package org.bytesoft.bytejta.xa;

import java.net.NetworkInterface;
import java.util.Calendar;
import java.util.Enumeration;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.bytesoft.common.utils.ByteUtils;
import org.bytesoft.transaction.xa.TransactionXid;
import org.bytesoft.transaction.xa.XidFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class XidFactoryImpl implements XidFactory {
	static final Logger logger = LoggerFactory.getLogger(XidFactoryImpl.class);

	static final int SIZE_OF_MAC = 6;
	static final Random random = new Random();
	static final byte[] hardwareAddress = new byte[SIZE_OF_MAC];
	static final AtomicInteger atomic = new AtomicInteger();

	static {
		byte[] sourceByteArray = getHardwareAddress();
		System.arraycopy(sourceByteArray, 0, hardwareAddress, 0, SIZE_OF_MAC);
	}

	private static byte[] getHardwareAddress() {
		Enumeration<NetworkInterface> enumeration = null;
		try {
			enumeration = NetworkInterface.getNetworkInterfaces();
		} catch (Exception ex) {
			logger.debug(ex.getMessage(), ex);
		}

		byte[] byteArray = null;
		while (byteArray == null && enumeration != null && enumeration.hasMoreElements()) {
			NetworkInterface element = enumeration.nextElement();

			try {
				if (element.isUp() == false) {
					continue;
				} else if (element.isPointToPoint() || element.isVirtual()) {
					continue;
				} else if (element.isLoopback()) {
					continue;
				}

				byte[] hardwareAddr = element.getHardwareAddress();
				if (hardwareAddr == null || hardwareAddr.length != SIZE_OF_MAC) {
					continue;
				}

				byteArray = new byte[SIZE_OF_MAC];
				System.arraycopy(hardwareAddr, 0, byteArray, 0, SIZE_OF_MAC);
			} catch (Exception rex) {
				logger.debug(rex.getMessage(), rex);
			}
		}

		return byteArray != null ? byteArray : new byte[SIZE_OF_MAC];
	}

	public TransactionXid createGlobalXid() {
		byte[] unique = this.generateUniqueKey();
		if (unique == null || unique.length != GLOBAL_TRANSACTION_LENGTH) {
			throw new IllegalStateException("The length of globalTransactionId not equals to 16.");
		}

		byte[] global = new byte[GLOBAL_TRANSACTION_LENGTH];
		System.arraycopy(unique, 0, global, 0, global.length);

		return new TransactionXid(XidFactory.JTA_FORMAT_ID, global);
	}

	public TransactionXid createGlobalXid(byte[] globalTransactionId) {
		if (globalTransactionId == null) {
			throw new IllegalArgumentException("The globalTransactionId cannot be null.");
		} else if (globalTransactionId.length > TransactionXid.MAXGTRIDSIZE) {
			throw new IllegalArgumentException("The length of globalTransactionId cannot exceed 64 bytes.");
		}
		byte[] global = new byte[globalTransactionId.length];
		System.arraycopy(globalTransactionId, 0, global, 0, global.length);
		return new TransactionXid(XidFactory.JTA_FORMAT_ID, global);
	}

	public TransactionXid createBranchXid(TransactionXid globalXid) {
		if (globalXid == null) {
			throw new IllegalArgumentException("Xid cannot be null.");
		} else if (globalXid.getGlobalTransactionId() == null) {
			throw new IllegalArgumentException("The globalTransactionId cannot be null.");
		} else if (globalXid.getGlobalTransactionId().length > TransactionXid.MAXGTRIDSIZE) {
			throw new IllegalArgumentException("The length of globalTransactionId cannot exceed 64 bytes.");
		}

		byte[] global = new byte[globalXid.getGlobalTransactionId().length];
		System.arraycopy(globalXid.getGlobalTransactionId(), 0, global, 0, global.length);

		byte[] unique = this.generateUniqueKey();
		if (unique == null || unique.length != BRANCH_QUALIFIER_LENGTH) {
			throw new IllegalStateException("The length of branchQulifier not equals to 16.");
		}

		byte[] branch = new byte[BRANCH_QUALIFIER_LENGTH];
		System.arraycopy(unique, 0, branch, 0, branch.length);

		return new TransactionXid(XidFactory.JTA_FORMAT_ID, global, branch);
	}

	public TransactionXid createBranchXid(TransactionXid globalXid, byte[] branchQualifier) {
		if (globalXid == null) {
			throw new IllegalArgumentException("Xid cannot be null.");
		} else if (globalXid.getGlobalTransactionId() == null) {
			throw new IllegalArgumentException("The globalTransactionId cannot be null.");
		} else if (globalXid.getGlobalTransactionId().length > TransactionXid.MAXGTRIDSIZE) {
			throw new IllegalArgumentException("The length of globalTransactionId cannot exceed 64 bytes.");
		}

		if (branchQualifier == null) {
			throw new IllegalArgumentException("The branchQulifier cannot be null.");
		} else if (branchQualifier.length > TransactionXid.MAXBQUALSIZE) {
			throw new IllegalArgumentException("The length of branchQulifier cannot exceed 64 bytes.");
		}

		byte[] global = new byte[globalXid.getGlobalTransactionId().length];
		System.arraycopy(globalXid.getGlobalTransactionId(), 0, global, 0, global.length);

		return new TransactionXid(XidFactory.JTA_FORMAT_ID, global, branchQualifier);
	}

	public byte[] generateUniqueKey() {
		byte[] byteArray = new byte[16];

		Calendar calendar = Calendar.getInstance();
		calendar.setTimeInMillis(System.currentTimeMillis());
		int year = calendar.get(Calendar.YEAR) - 2014;
		int month = calendar.get(Calendar.MONTH) + 1;
		int day = calendar.get(Calendar.DAY_OF_MONTH);
		int hour = calendar.get(Calendar.HOUR_OF_DAY);
		int minute = calendar.get(Calendar.MINUTE);
		int second = calendar.get(Calendar.SECOND);
		int millis = calendar.get(Calendar.MILLISECOND);

		int value = (year << 28);
		value = value | (month << 24);
		value = value | (day << 19);
		value = value | (hour << 14);
		value = value | (minute << 8);
		value = value | (second << 2);
		value = value | (millis >>> 8);

		byte[] valueByteArray = ByteUtils.intToByteArray(value);

		byte[] timeByteArray = new byte[5];
		System.arraycopy(valueByteArray, 0, timeByteArray, 0, 4);
		timeByteArray[4] = (byte) (((millis << 24) >>> 24) + Byte.MIN_VALUE);

		byte increment = (byte) atomic.incrementAndGet();

		byte[] randomByteArray = new byte[4];
		random.nextBytes(randomByteArray);

		System.arraycopy(hardwareAddress, 0, byteArray, 0, SIZE_OF_MAC);
		System.arraycopy(timeByteArray, 0, byteArray, SIZE_OF_MAC, 5);
		byteArray[SIZE_OF_MAC + 5] = increment;
		System.arraycopy(randomByteArray, 0, byteArray, SIZE_OF_MAC + 5 + 1, randomByteArray.length);

		return byteArray;
	}

}
