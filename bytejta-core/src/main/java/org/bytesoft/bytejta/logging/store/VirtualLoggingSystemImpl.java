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
package org.bytesoft.bytejta.logging.store;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.resource.spi.work.Work;
import javax.transaction.xa.Xid;

import org.bytesoft.common.utils.ByteUtils;
import org.bytesoft.transaction.logging.store.VirtualLoggingKey;
import org.bytesoft.transaction.logging.store.VirtualLoggingListener;
import org.bytesoft.transaction.logging.store.VirtualLoggingRecord;
import org.bytesoft.transaction.logging.store.VirtualLoggingSystem;
import org.bytesoft.transaction.logging.store.VirtualLoggingTrigger;
import org.bytesoft.transaction.xa.XidFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class VirtualLoggingSystemImpl implements VirtualLoggingSystem, VirtualLoggingTrigger, Work {
	static final Logger logger = LoggerFactory.getLogger(VirtualLoggingSystemImpl.class);

	private final Lock lock = new ReentrantLock();
	private final Lock timingLock = new ReentrantLock();
	private final Condition timingCondition = this.timingLock.newCondition();

	private boolean released;

	private File directory;

	private VirtualLoggingFile master;
	private VirtualLoggingFile slaver;

	public void construct() throws IOException {
		if (this.directory == null) {
			this.directory = this.getDefaultDirectory();
		}

		if (this.directory.exists() == false) {
			if (this.directory.mkdirs() == false) {
				throw new RuntimeException();
			}
		}
		File fmaster = new File(this.directory, String.format("%s1.log", this.getLoggingFilePrefix()));
		File fslaver = new File(this.directory, String.format("%s2.log", this.getLoggingFilePrefix()));

		VirtualLoggingFile masterMgr = this.createTransactionLogging(fmaster);
		VirtualLoggingFile slaverMgr = this.createTransactionLogging(fslaver);

		masterMgr.initialize(true);
		slaverMgr.initialize(false);

		this.initialize(masterMgr, slaverMgr);
	}

	private void initialize(VirtualLoggingFile prev, VirtualLoggingFile next) {
		boolean prevMaster = prev.isMaster();
		boolean nextMaster = next.isMaster();

		if (prevMaster && nextMaster) {
			throw new IllegalStateException();
		} else if (prevMaster == false && nextMaster == false) {
			this.fixSwitchError(prev, next);
		} else if (prevMaster) {
			this.master = prev;
			this.slaver = next;

			this.master.clearMarkedFlag();
			this.slaver.clearMarkedFlag();
		} else {
			this.master = next;
			this.slaver = prev;

			this.master.clearMarkedFlag();
			this.slaver.clearMarkedFlag();
		}
	}

	private void fixSwitchError(VirtualLoggingFile prev, VirtualLoggingFile next) {
		boolean prevMarked = prev.isMarked();
		boolean nextMarked = next.isMarked();
		if (prevMarked && nextMarked) {
			throw new IllegalStateException();
		} else if (prevMarked == false && nextMarked == false) {
			throw new IllegalStateException();
		} else if (prevMarked) {
			prev.fixSwitchError();

			this.master = prev;
			this.slaver = next;
		} else {
			next.fixSwitchError();

			this.master = next;
			this.slaver = prev;
		}
	}

	public void run() {
		while (this.released == false) {
			try {
				this.timingLock.lock();
				this.timingCondition.await(30, TimeUnit.SECONDS);
			} catch (Exception ex) {
				logger.debug(ex.getMessage(), ex);
			} finally {
				this.timingLock.unlock();
			}

			this.syncMasterAndSlaver();
			this.swapMasterAndSlaver();

		}
	}

	public void fireSwapImmediately() {
		try {
			this.timingLock.lock();
			this.timingCondition.signalAll();
		} finally {
			this.timingLock.unlock();
		}
	}

	public void traversal(VirtualLoggingListener listener) {
		this.master.prepareForReading();
		while (true) {
			byte[] byteArray = null;
			try {
				byteArray = this.master.read();
			} catch (RuntimeException rex) {
				byteArray = new byte[0];
			}

			if (byteArray.length == 0) {
				break;
			}

			byte[] keyByteArray = new byte[XidFactory.GLOBAL_TRANSACTION_LENGTH];
			System.arraycopy(byteArray, 0, keyByteArray, 0, keyByteArray.length);
			int operator = byteArray[keyByteArray.length];
			byte[] valueByteArray = new byte[byteArray.length - XidFactory.GLOBAL_TRANSACTION_LENGTH - 1 - 4];
			System.arraycopy(byteArray, XidFactory.GLOBAL_TRANSACTION_LENGTH + 1 + 4, valueByteArray, 0, valueByteArray.length);

			VirtualLoggingKey xid = new VirtualLoggingKey();
			xid.setGlobalTransactionId(keyByteArray);

			VirtualLoggingRecord record = new VirtualLoggingRecord();
			record.setIdentifier(xid);
			record.setOperator(operator);
			record.setContent(byteArray);
			record.setValue(valueByteArray);

			listener.recvOperation(record);
		}

	}

	public void create(Xid xid, byte[] textByteArray) {
		byte[] keyByteArray = xid.getGlobalTransactionId();
		byte[] sizeByteArray = ByteUtils.intToByteArray(textByteArray.length);

		byte[] byteArray = new byte[keyByteArray.length + 1 + sizeByteArray.length + textByteArray.length];

		System.arraycopy(keyByteArray, 0, byteArray, 0, keyByteArray.length);
		byteArray[keyByteArray.length] = (byte) (OPERATOR_CREATE & 0xFF);
		System.arraycopy(sizeByteArray, 0, byteArray, keyByteArray.length + 1, sizeByteArray.length);
		System.arraycopy(textByteArray, 0, byteArray, keyByteArray.length + 1 + sizeByteArray.length, textByteArray.length);

		try {
			this.lock.lock();
			this.master.write(byteArray);
		} finally {
			this.lock.unlock();
		}
	}

	public void delete(Xid xid) {
		byte[] keyByteArray = xid.getGlobalTransactionId();
		byte[] sizeByteArray = ByteUtils.intToByteArray(0);

		byte[] byteArray = new byte[keyByteArray.length + 1 + sizeByteArray.length];

		System.arraycopy(keyByteArray, 0, byteArray, 0, keyByteArray.length);
		byteArray[keyByteArray.length] = (byte) (OPERATOR_DELETE & 0xFF);
		System.arraycopy(sizeByteArray, 0, byteArray, keyByteArray.length + 1, sizeByteArray.length);

		try {
			this.lock.lock();
			this.master.write(byteArray);
		} finally {
			this.lock.unlock();
		}
	}

	public void modify(Xid xid, byte[] textByteArray) {
		byte[] keyByteArray = xid.getGlobalTransactionId();
		byte[] sizeByteArray = ByteUtils.intToByteArray(textByteArray.length);

		byte[] byteArray = new byte[keyByteArray.length + 1 + sizeByteArray.length + textByteArray.length];

		System.arraycopy(keyByteArray, 0, byteArray, 0, keyByteArray.length);
		byteArray[keyByteArray.length] = (byte) (OPERATOR_MOFIFY & 0xFF);
		System.arraycopy(sizeByteArray, 0, byteArray, keyByteArray.length + 1, sizeByteArray.length);
		System.arraycopy(textByteArray, 0, byteArray, keyByteArray.length + 1 + sizeByteArray.length, textByteArray.length);

		try {
			this.lock.lock();
			this.master.write(byteArray);
		} finally {
			this.lock.unlock();
		}
	}

	public void syncMasterAndSlaver() {
		this.master.prepareForReading();
		Map<Xid, Boolean> recordMap = this.syncStepOne();
		this.master.prepareForReading();
		this.syncStepTwo(recordMap);
	}

	public Map<Xid, Boolean> syncStepOne() {
		Map<Xid, Boolean> recordMap = new HashMap<Xid, Boolean>();
		while (true) {
			byte[] byteArray = null;
			try {
				byteArray = this.master.read();
			} catch (RuntimeException rex) {
				byteArray = new byte[0];
			}

			if (byteArray.length == 0) {
				break;
			}

			byte[] keyByteArray = new byte[XidFactory.GLOBAL_TRANSACTION_LENGTH];
			System.arraycopy(byteArray, 0, keyByteArray, 0, keyByteArray.length);
			int operator = byteArray[keyByteArray.length];

			VirtualLoggingKey xid = new VirtualLoggingKey();
			xid.setGlobalTransactionId(keyByteArray);

			VirtualLoggingRecord record = new VirtualLoggingRecord();
			record.setIdentifier(xid);
			record.setOperator(operator);
			record.setContent(byteArray);

			if (operator == OPERATOR_DELETE) {
				recordMap.put(xid, true);
			}

		}

		return recordMap;

	}

	public void syncStepTwo(Map<Xid, Boolean> recordMap) {
		List<VirtualLoggingRecord> recordList = new ArrayList<VirtualLoggingRecord>();
		while (true) {
			byte[] byteArray = null;
			try {
				byteArray = this.master.read();
			} catch (RuntimeException rex) {
				byteArray = new byte[0];
			}

			if (byteArray.length == 0) {
				break;
			}

			byte[] keyByteArray = new byte[XidFactory.GLOBAL_TRANSACTION_LENGTH];
			System.arraycopy(byteArray, 0, keyByteArray, 0, keyByteArray.length);
			int operator = byteArray[keyByteArray.length];

			VirtualLoggingKey xid = new VirtualLoggingKey();
			xid.setGlobalTransactionId(keyByteArray);

			VirtualLoggingRecord record = new VirtualLoggingRecord();
			record.setIdentifier(xid);
			record.setOperator(operator);
			record.setContent(byteArray);

			if (recordMap.containsKey(xid) == false) {
				recordList.add(record);
			}

		}

		for (int i = 0; i < recordList.size(); i++) {
			VirtualLoggingRecord record = recordList.get(i);
			Xid xid = record.getIdentifier();
			if (recordMap.containsKey(xid)) {
				byte[] byteArray = record.getContent();
				this.slaver.write(byteArray);
			}
		}

	}

	public void swapMasterAndSlaver() {
		try {
			this.lock.lock();
			this.syncStepTwo(new HashMap<Xid, Boolean>());

			this.slaver.markAsMaster();
			this.master.switchToSlaver();
			this.slaver.switchToMaster();

			VirtualLoggingFile theNextMaster = this.slaver;
			this.slaver = this.master;
			this.master = theNextMaster;

		} finally {
			this.lock.unlock();
		}
	}

	public void flushImmediately() {
		this.master.flushImmediately();
	}

	public void shutdown() {
		this.master.flushImmediately();
		this.slaver.flushImmediately();

		this.master.closeQuietly();
		this.slaver.closeQuietly();
	}

	public void release() {
		this.released = true;
	}

	public abstract File getDefaultDirectory();

	public abstract String getLoggingIdentifier();

	public abstract String getLoggingFilePrefix();

	public VirtualLoggingFile createTransactionLogging(File file) throws IOException {
		VirtualLoggingFile logging = new VirtualLoggingFile(file);
		logging.setTrigger(this);
		logging.setIdentifier(this.getLoggingIdentifier().getBytes());
		return logging;
	}

	public File getDirectory() {
		return directory;
	}

	public void setDirectory(File directory) {
		this.directory = directory;
	}

}
