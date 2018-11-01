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
package org.bytesoft.bytejta.logging;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.transaction.xa.Xid;

import org.apache.commons.lang3.StringUtils;
import org.bytesoft.bytejta.logging.store.VirtualLoggingSystemImpl;
import org.bytesoft.common.utils.ByteUtils;
import org.bytesoft.transaction.TransactionBeanFactory;
import org.bytesoft.transaction.archive.TransactionArchive;
import org.bytesoft.transaction.archive.XAResourceArchive;
import org.bytesoft.transaction.aware.TransactionBeanFactoryAware;
import org.bytesoft.transaction.aware.TransactionEndpointAware;
import org.bytesoft.transaction.logging.ArchiveDeserializer;
import org.bytesoft.transaction.logging.LoggingFlushable;
import org.bytesoft.transaction.logging.TransactionLogger;
import org.bytesoft.transaction.logging.store.VirtualLoggingListener;
import org.bytesoft.transaction.logging.store.VirtualLoggingRecord;
import org.bytesoft.transaction.logging.store.VirtualLoggingSystem;
import org.bytesoft.transaction.recovery.TransactionRecoveryCallback;
import org.bytesoft.transaction.xa.TransactionXid;
import org.bytesoft.transaction.xa.XidFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SampleTransactionLogger extends VirtualLoggingSystemImpl
		implements TransactionLogger, LoggingFlushable, TransactionBeanFactoryAware, TransactionEndpointAware {
	static final Logger logger = LoggerFactory.getLogger(SampleTransactionLogger.class);

	@javax.inject.Inject
	private TransactionBeanFactory beanFactory;
	private String identifier;

	@PostConstruct
	public void construct() throws IOException {
		this.initializeIfNecessary();
	}

	private void initializeIfNecessary() throws IllegalStateException {
		if (StringUtils.isNotBlank(this.identifier)) {
			try {
				super.construct();
			} catch (IOException error) {
				throw new IllegalStateException("Error occurred while initializing tx-log!", error);
			}
		} // end-if (StringUtils.isNotBlank(this.endpoint))
	}

	public void createTransaction(TransactionArchive archive) {
		ArchiveDeserializer deserializer = this.beanFactory.getArchiveDeserializer();

		try {
			byte[] byteArray = deserializer.serialize((TransactionXid) archive.getXid(), archive);
			this.create(archive.getXid(), byteArray);
		} catch (RuntimeException rex) {
			logger.error("Error occurred while creating transaction-archive.", rex);
		}
	}

	public void updateTransaction(TransactionArchive archive) {
		ArchiveDeserializer deserializer = this.beanFactory.getArchiveDeserializer();

		try {
			byte[] byteArray = deserializer.serialize((TransactionXid) archive.getXid(), archive);
			this.modify(archive.getXid(), byteArray);
		} catch (RuntimeException rex) {
			logger.error("Error occurred while modifying transaction-archive.", rex);
		}
	}

	public void deleteTransaction(TransactionArchive archive) {
		try {
			this.delete(archive.getXid());
		} catch (RuntimeException rex) {
			logger.error("Error occurred while deleting transaction-archive.", rex);
		}
	}

	public void createParticipant(XAResourceArchive archive) {
	}

	public void updateParticipant(XAResourceArchive archive) {
		ArchiveDeserializer deserializer = this.beanFactory.getArchiveDeserializer();

		try {
			byte[] byteArray = deserializer.serialize((TransactionXid) archive.getXid(), archive);
			this.modify(archive.getXid(), byteArray);
		} catch (RuntimeException rex) {
			logger.error("Error occurred while modifying resource-archive.", rex);
		}
	}

	public void deleteParticipant(XAResourceArchive archive) {
	}

	public void createResource(XAResourceArchive archive) {
	}

	public void updateResource(XAResourceArchive archive) {
	}

	public void deleteResource(XAResourceArchive archive) {
	}

	public List<VirtualLoggingRecord> compressIfNecessary(List<VirtualLoggingRecord> recordList) {
		ArchiveDeserializer deserializer = this.beanFactory.getArchiveDeserializer();
		XidFactory xidFactory = this.beanFactory.getXidFactory();

		List<VirtualLoggingRecord> resultList = new ArrayList<VirtualLoggingRecord>();

		Map<TransactionXid, TransactionArchive> xidMap = new HashMap<TransactionXid, TransactionArchive>();
		for (int index = 0; recordList != null && index < recordList.size(); index++) {
			VirtualLoggingRecord record = recordList.get(index);
			byte[] byteArray = record.getContent();
			byte[] keyByteArray = new byte[XidFactory.GLOBAL_TRANSACTION_LENGTH];
			System.arraycopy(byteArray, 0, keyByteArray, 0, keyByteArray.length);
			// int operator = byteArray[keyByteArray.length];
			byte[] valueByteArray = new byte[byteArray.length - XidFactory.GLOBAL_TRANSACTION_LENGTH - 1 - 4];
			System.arraycopy(byteArray, XidFactory.GLOBAL_TRANSACTION_LENGTH + 1 + 4, valueByteArray, 0, valueByteArray.length);

			TransactionXid xid = xidFactory.createGlobalXid(keyByteArray);

			Object obj = deserializer.deserialize(xid, valueByteArray);
			if (TransactionArchive.class.isInstance(obj)) {
				xidMap.put(xid, (TransactionArchive) obj);
			} else if (XAResourceArchive.class.isInstance(obj)) {
				TransactionArchive archive = xidMap.get(xid);
				if (archive == null) {
					logger.error("Error occurred while compressing resource archive: {}", obj);
					continue;
				}

				XAResourceArchive resourceArchive = (XAResourceArchive) obj;
				boolean matched = false;

				List<XAResourceArchive> nativeResources = archive.getNativeResources();
				for (int i = 0; matched == false && nativeResources != null && i < nativeResources.size(); i++) {
					XAResourceArchive element = nativeResources.get(i);
					if (resourceArchive.getXid().equals(element.getXid())) {
						matched = true;
						nativeResources.set(i, resourceArchive);
					}
				}

				XAResourceArchive optimizedResource = archive.getOptimizedResource();
				if (matched == false && optimizedResource != null) {
					if (resourceArchive.getXid().equals(optimizedResource.getXid())) {
						matched = true;
						archive.setOptimizedResource(resourceArchive);
					}
				}

				List<XAResourceArchive> remoteResources = archive.getRemoteResources();
				for (int i = 0; matched == false && remoteResources != null && i < remoteResources.size(); i++) {
					XAResourceArchive element = remoteResources.get(i);
					if (resourceArchive.getXid().equals(element.getXid())) {
						matched = true;
						remoteResources.set(i, resourceArchive);
					}
				}

				if (matched == false) {
					logger.error("Error occurred while compressing resource archive: {}, invalid resoure!", obj);
				}

			} else {
				logger.error("unkown resource: {}!", obj);
			}
		}

		for (Iterator<Map.Entry<TransactionXid, TransactionArchive>> itr = xidMap.entrySet().iterator(); itr.hasNext();) {
			Map.Entry<TransactionXid, TransactionArchive> entry = itr.next();
			TransactionXid xid = entry.getKey();
			TransactionArchive value = entry.getValue();

			byte[] globalByteArray = xid.getGlobalTransactionId();

			byte[] keyByteArray = new byte[XidFactory.GLOBAL_TRANSACTION_LENGTH];
			byte[] valueByteArray = deserializer.serialize(xid, value);
			byte[] sizeByteArray = ByteUtils.intToByteArray(valueByteArray.length);

			System.arraycopy(globalByteArray, 0, keyByteArray, 0, XidFactory.GLOBAL_TRANSACTION_LENGTH);

			byte[] byteArray = new byte[XidFactory.GLOBAL_TRANSACTION_LENGTH + 1 + 4 + valueByteArray.length];

			System.arraycopy(keyByteArray, 0, byteArray, 0, keyByteArray.length);
			byteArray[keyByteArray.length] = OPERATOR_CREATE;
			System.arraycopy(sizeByteArray, 0, byteArray, XidFactory.GLOBAL_TRANSACTION_LENGTH + 1, sizeByteArray.length);
			System.arraycopy(valueByteArray, 0, byteArray, XidFactory.GLOBAL_TRANSACTION_LENGTH + 1 + 4, valueByteArray.length);

			VirtualLoggingRecord record = new VirtualLoggingRecord();
			record.setIdentifier(xid);
			record.setOperator(OPERATOR_CREATE);
			record.setValue(valueByteArray);
			record.setContent(byteArray);

			resultList.add(record);
		}

		return resultList;
	}

	public void recover(TransactionRecoveryCallback callback) {

		final Map<Xid, TransactionArchive> xidMap = new HashMap<Xid, TransactionArchive>();
		final ArchiveDeserializer deserializer = this.beanFactory.getArchiveDeserializer();
		final XidFactory xidFactory = this.beanFactory.getXidFactory();

		this.traversal(new VirtualLoggingListener() {
			public void recvOperation(VirtualLoggingRecord action) {
				Xid xid = action.getIdentifier();
				int operator = action.getOperator();
				if (VirtualLoggingSystem.OPERATOR_DELETE == operator) {
					xidMap.remove(xid);
				} else if (xidMap.containsKey(xid) == false) {
					xidMap.put(xid, null);
				}
			}
		});

		this.traversal(new VirtualLoggingListener() {
			public void recvOperation(VirtualLoggingRecord action) {
				Xid xid = action.getIdentifier();
				if (xidMap.containsKey(xid)) {
					this.execOperation(action);
				}
			}

			public void execOperation(VirtualLoggingRecord action) {
				Xid identifier = action.getIdentifier();

				TransactionXid xid = xidFactory.createGlobalXid(identifier.getGlobalTransactionId());

				Object obj = deserializer.deserialize(xid, action.getValue());
				if (TransactionArchive.class.isInstance(obj)) {
					TransactionArchive archive = (TransactionArchive) obj;
					xidMap.put(identifier, archive);
				} else if (XAResourceArchive.class.isInstance(obj)) {
					TransactionArchive archive = xidMap.get(identifier);
					if (archive == null) {
						logger.error("Error occurred while recovering resource archive: {}", obj);
						return;
					}

					XAResourceArchive resourceArchive = (XAResourceArchive) obj;
					boolean matched = false;

					List<XAResourceArchive> nativeResources = archive.getNativeResources();
					for (int i = 0; matched == false && nativeResources != null && i < nativeResources.size(); i++) {
						XAResourceArchive element = nativeResources.get(i);
						if (resourceArchive.getXid().equals(element.getXid())) {
							matched = true;
							nativeResources.set(i, resourceArchive);
						}
					}

					XAResourceArchive optimizedResource = archive.getOptimizedResource();
					if (matched == false && optimizedResource != null) {
						if (resourceArchive.getXid().equals(optimizedResource.getXid())) {
							matched = true;
							archive.setOptimizedResource(resourceArchive);
						}
					}

					List<XAResourceArchive> remoteResources = archive.getRemoteResources();
					for (int i = 0; matched == false && remoteResources != null && i < remoteResources.size(); i++) {
						XAResourceArchive element = remoteResources.get(i);
						if (resourceArchive.getXid().equals(element.getXid())) {
							matched = true;
							remoteResources.set(i, resourceArchive);
						}
					}

					if (matched == false) {
						logger.error("Error occurred while recovering resource archive: {}, invalid resoure!", obj);
					}

				}

			}
		});

		for (Iterator<Map.Entry<Xid, TransactionArchive>> itr = xidMap.entrySet().iterator(); itr.hasNext();) {
			Map.Entry<Xid, TransactionArchive> entry = itr.next();
			TransactionArchive archive = entry.getValue();
			if (archive == null) {
				continue;
			} else {
				try {
					callback.recover(archive);
				} catch (RuntimeException rex) {
					logger.error("Error occurred while recovering transaction(xid= {}).", archive.getXid(), rex);
				}
			}
		}

	}

	public File getDefaultDirectory() {
		String address = StringUtils.trimToEmpty(this.identifier);
		File directory = new File(String.format("bytejta/%s", address.replaceAll("\\W", "_")));
		if (directory.exists() == false) {
			try {
				boolean created = directory.mkdirs();
				if (created == false) {
					logger.error("Failed to create directory {}!", directory.getAbsolutePath());
				} // end-if (created == false)
			} catch (SecurityException ex) {
				logger.error("Error occurred while creating directory {}!", directory.getAbsolutePath(), ex);
			}
		}
		return directory;
	}

	public int getMajorVersion() {
		return 0;
	}

	public int getMinorVersion() {
		return 6;
	}

	public String getLoggingFilePrefix() {
		return "bytejta-";
	}

	public String getLoggingIdentifier() {
		return "org.bytesoft.bytejta.logging.sample";
	}

	public String getEndpoint() {
		return identifier;
	}

	public void setEndpoint(String identifier) {
		this.identifier = identifier;
		this.initializeIfNecessary();
	}

	public TransactionBeanFactory getBeanFactory() {
		return beanFactory;
	}

	public void setBeanFactory(TransactionBeanFactory beanFactory) {
		this.beanFactory = beanFactory;
	}

}
