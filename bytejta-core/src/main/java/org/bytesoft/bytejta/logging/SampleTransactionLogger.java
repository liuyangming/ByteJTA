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

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.transaction.xa.Xid;

import org.bytesoft.bytejta.logging.store.VirtualLoggingSystemImpl;
import org.bytesoft.transaction.TransactionBeanFactory;
import org.bytesoft.transaction.archive.TransactionArchive;
import org.bytesoft.transaction.archive.XAResourceArchive;
import org.bytesoft.transaction.aware.TransactionBeanFactoryAware;
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
		implements TransactionLogger, LoggingFlushable, TransactionBeanFactoryAware {
	static final Logger logger = LoggerFactory.getLogger(SampleTransactionLogger.class.getSimpleName());

	private TransactionBeanFactory beanFactory;

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

	public void updateResource(XAResourceArchive archive) {
		ArchiveDeserializer deserializer = this.beanFactory.getArchiveDeserializer();

		try {
			byte[] byteArray = deserializer.serialize((TransactionXid) archive.getXid(), archive);
			this.modify(archive.getXid(), byteArray);
		} catch (RuntimeException rex) {
			logger.error("Error occurred while modifying resource-archive.", rex);
		}
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

					List<XAResourceArchive> remoteResources = archive.getRemoteResources();
					for (int i = 0; matched == false && remoteResources != null && i < remoteResources.size(); i++) {
						XAResourceArchive element = remoteResources.get(i);
						if (resourceArchive.getXid().equals(element.getXid())) {
							matched = true;
							remoteResources.set(i, resourceArchive);
						}
					}

				}

			}
		});

		for (Iterator<Map.Entry<Xid, TransactionArchive>> itr = xidMap.entrySet().iterator(); itr.hasNext();) {
			Map.Entry<Xid, TransactionArchive> entry = itr.next();
			TransactionArchive archive = entry.getValue();
			try {
				callback.recover(archive);
			} catch (RuntimeException rex) {
				logger.error("Error occurred while recovering transaction(xid= {}).", archive.getXid(), rex);
			}
		}

	}

	public String getLoggingFilePrefix() {
		return "bytejta-";
	}

	public String getLoggingIdentifier() {
		return "org.bytesoft.bytejta.logging.sample";
	}

	public TransactionBeanFactory getBeanFactory() {
		return beanFactory;
	}

	public void setBeanFactory(TransactionBeanFactory beanFactory) {
		this.beanFactory = beanFactory;
	}

}
