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
package org.bytesoft.bytejta;

import java.util.List;
import java.util.Map;

import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.xa.XAResource;

import org.apache.commons.lang3.StringUtils;
import org.bytesoft.bytejta.supports.resource.RemoteResourceDescriptor;
import org.bytesoft.common.utils.ByteUtils;
import org.bytesoft.common.utils.CommonUtils;
import org.bytesoft.transaction.CommitRequiredException;
import org.bytesoft.transaction.RollbackRequiredException;
import org.bytesoft.transaction.Transaction;
import org.bytesoft.transaction.TransactionBeanFactory;
import org.bytesoft.transaction.TransactionContext;
import org.bytesoft.transaction.TransactionRecovery;
import org.bytesoft.transaction.TransactionRepository;
import org.bytesoft.transaction.archive.TransactionArchive;
import org.bytesoft.transaction.archive.XAResourceArchive;
import org.bytesoft.transaction.aware.TransactionBeanFactoryAware;
import org.bytesoft.transaction.logging.TransactionLogger;
import org.bytesoft.transaction.recovery.TransactionRecoveryCallback;
import org.bytesoft.transaction.recovery.TransactionRecoveryListener;
import org.bytesoft.transaction.remote.RemoteSvc;
import org.bytesoft.transaction.supports.resource.XAResourceDescriptor;
import org.bytesoft.transaction.xa.TransactionXid;
import org.bytesoft.transaction.xa.XidFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransactionRecoveryImpl implements TransactionRecovery, TransactionBeanFactoryAware {
	static final Logger logger = LoggerFactory.getLogger(TransactionRecoveryImpl.class);
	static final long SECOND_MILLIS = 1000L;

	private TransactionRecoveryListener listener;
	@javax.inject.Inject
	private TransactionBeanFactory beanFactory;

	public synchronized void timingRecover() {
		TransactionRepository transactionRepository = beanFactory.getTransactionRepository();
		List<Transaction> transactions = transactionRepository.getErrorTransactionList();
		int total = transactions == null ? 0 : transactions.size(), value = 0;
		for (int i = 0; transactions != null && i < transactions.size(); i++) {
			Transaction transaction = transactions.get(i);
			TransactionContext transactionContext = transaction.getTransactionContext();
			TransactionXid xid = transactionContext.getXid();
			int recoveredTimes = transactionContext.getRecoveredTimes() > 10 ? 10 : transactionContext.getRecoveredTimes();
			long recoverMillis = transactionContext.getCreatedTime() + SECOND_MILLIS * 60L * (long) Math.pow(2, recoveredTimes);

			if (System.currentTimeMillis() < recoverMillis) {
				continue;
			} // end-if (System.currentTimeMillis() < recoverMillis)

			try {
				this.recoverTransaction(transaction);
				value++;
			} catch (CommitRequiredException ex) {
				logger.debug("{}> recover: branch={}, message= commit-required",
						ByteUtils.byteArrayToString(xid.getGlobalTransactionId()),
						ByteUtils.byteArrayToString(xid.getBranchQualifier()), ex);
				continue;
			} catch (RollbackRequiredException ex) {
				logger.debug("{}> recover: branch={}, message= rollback-required",
						ByteUtils.byteArrayToString(xid.getGlobalTransactionId()),
						ByteUtils.byteArrayToString(xid.getBranchQualifier()), ex);
				continue;
			} catch (SystemException ex) {
				logger.debug("{}> recover: branch={}, message= {}", ByteUtils.byteArrayToString(xid.getGlobalTransactionId()),
						ByteUtils.byteArrayToString(xid.getBranchQualifier()), ex.getMessage(), ex);
				continue;
			} catch (RuntimeException ex) {
				logger.debug("{}> recover: branch={}, message= {}", ByteUtils.byteArrayToString(xid.getGlobalTransactionId()),
						ByteUtils.byteArrayToString(xid.getBranchQualifier()), ex.getMessage(), ex);
				continue;
			}
		}
		logger.debug("[transaction-recovery] total= {}, success= {}", total, value);
	}

	public void recoverTransaction(Transaction transaction)
			throws CommitRequiredException, RollbackRequiredException, SystemException {

		TransactionContext transactionContext = transaction.getTransactionContext();
		boolean coordinator = transactionContext.isCoordinator();
		if (coordinator) {
			transaction.recover();
			this.recoverCoordinator(transaction);
		} else {
			transaction.recover();
			this.recoverParticipant(transaction);
		}

	}

	protected void recoverCoordinator(Transaction transaction)
			throws CommitRequiredException, RollbackRequiredException, SystemException {

		switch (transaction.getTransactionStatus()) {
		case Status.STATUS_ACTIVE:
		case Status.STATUS_MARKED_ROLLBACK:
		case Status.STATUS_PREPARING:
		case Status.STATUS_ROLLING_BACK:
		case Status.STATUS_UNKNOWN:
			transaction.recoveryRollback();
			transaction.forgetQuietly();
			break;
		case Status.STATUS_PREPARED:
		case Status.STATUS_COMMITTING:
			transaction.recoveryCommit();
			transaction.forgetQuietly();
			break;
		case Status.STATUS_COMMITTED:
		case Status.STATUS_ROLLEDBACK:
			transaction.forgetQuietly();
			break;
		default:
			logger.debug("Current transaction has already been completed.");
		}
	}

	protected void recoverParticipant(Transaction transaction)
			throws CommitRequiredException, RollbackRequiredException, SystemException {

		TransactionImpl transactionImpl = (TransactionImpl) transaction;
		switch (transaction.getTransactionStatus()) {
		case Status.STATUS_PREPARED:
		case Status.STATUS_COMMITTING:
			break;
		case Status.STATUS_COMMITTED:
		case Status.STATUS_ROLLEDBACK:
			break;
		case Status.STATUS_ACTIVE:
		case Status.STATUS_MARKED_ROLLBACK:
		case Status.STATUS_PREPARING:
		case Status.STATUS_UNKNOWN:
		case Status.STATUS_ROLLING_BACK:
		default:
			transactionImpl.recoveryRollback();
			transactionImpl.forgetQuietly();
		}
	}

	public synchronized void startRecovery() {
		final TransactionRepository transactionRepository = beanFactory.getTransactionRepository();
		final TransactionLogger transactionLogger = beanFactory.getTransactionLogger();
		transactionLogger.recover(new TransactionRecoveryCallback() {

			public void recover(TransactionArchive archive) {
				try {
					TransactionImpl transaction = (TransactionImpl) reconstruct(archive);
					if (listener != null) {
						listener.onRecovery(transaction);
					}
					TransactionContext transactionContext = transaction.getTransactionContext();
					TransactionXid globalXid = transactionContext.getXid();
					transactionRepository.putTransaction(globalXid, transaction);
					transactionRepository.putErrorTransaction(globalXid, transaction);
				} catch (IllegalStateException ex) {
					transactionLogger.deleteTransaction(archive);
				}

			}
		});

		TransactionCoordinator transactionCoordinator = //
				(TransactionCoordinator) this.beanFactory.getNativeParticipant();
		transactionCoordinator.markParticipantReady();
	}

	public org.bytesoft.transaction.Transaction reconstruct(TransactionArchive archive) throws IllegalStateException {
		XidFactory xidFactory = this.beanFactory.getXidFactory();
		TransactionContext transactionContext = new TransactionContext();
		TransactionXid xid = (TransactionXid) archive.getXid();
		transactionContext.setXid(xidFactory.createGlobalXid(xid.getGlobalTransactionId()));
		transactionContext.setRecoveried(true);
		transactionContext.setCoordinator(archive.isCoordinator());
		transactionContext.setPropagatedBy(archive.getPropagatedBy());
		transactionContext.setRecoveredTimes(archive.getRecoveredTimes());
		transactionContext.setCreatedTime(archive.getRecoveredAt());

		TransactionImpl transaction = new TransactionImpl(transactionContext);
		transaction.setBeanFactory(this.beanFactory);
		transaction.setTransactionStatus(archive.getStatus());

		List<XAResourceArchive> nativeResources = archive.getNativeResources();
		transaction.getNativeParticipantList().addAll(nativeResources);

		transaction.setParticipant(archive.getOptimizedResource());

		List<XAResourceArchive> remoteResources = archive.getRemoteResources();
		transaction.getRemoteParticipantList().addAll(remoteResources);

		List<XAResourceArchive> participants = transaction.getParticipantList();
		Map<String, XAResourceArchive> nativeParticipantMap = transaction.getNativeParticipantMap();
		Map<RemoteSvc, XAResourceArchive> remoteParticipantMap = transaction.getRemoteParticipantMap();

		if (archive.getOptimizedResource() != null) {
			XAResourceArchive optimized = archive.getOptimizedResource();
			XAResourceDescriptor descriptor = optimized.getDescriptor();
			String identifier = descriptor.getIdentifier();
			if (RemoteResourceDescriptor.class.isInstance(descriptor)) {
				RemoteSvc remoteSvc = CommonUtils.getRemoteSvc(identifier);
				remoteParticipantMap.put(remoteSvc, optimized);
			} else {
				nativeParticipantMap.put(identifier, optimized);
			}

			participants.add(optimized);
		}

		for (int i = 0; i < nativeResources.size(); i++) {
			XAResourceArchive element = nativeResources.get(i);
			XAResourceDescriptor descriptor = element.getDescriptor();
			String identifier = StringUtils.trimToEmpty(descriptor.getIdentifier());
			nativeParticipantMap.put(identifier, element);
		}
		participants.addAll(nativeResources);

		for (int i = 0; i < remoteResources.size(); i++) {
			XAResourceArchive element = remoteResources.get(i);
			XAResourceDescriptor descriptor = element.getDescriptor();
			String identifier = StringUtils.trimToEmpty(descriptor.getIdentifier());

			if (RemoteResourceDescriptor.class.isInstance(descriptor)) {
				RemoteSvc remoteSvc = CommonUtils.getRemoteSvc(identifier);
				remoteParticipantMap.put(remoteSvc, element);
			} // end-if (RemoteResourceDescriptor.class.isInstance(descriptor))
		}
		participants.addAll(remoteResources);

		transaction.recoverTransactionStrategy(archive.getTransactionStrategyType());

		if (archive.getVote() == XAResource.XA_RDONLY) {
			throw new IllegalStateException("Transaction has already been completed!");
		}

		return transaction;
	}

	public TransactionBeanFactory getBeanFactory() {
		return beanFactory;
	}

	public void setBeanFactory(TransactionBeanFactory tbf) {
		this.beanFactory = tbf;
	}

	public TransactionRecoveryListener getListener() {
		return listener;
	}

	public void setListener(TransactionRecoveryListener listener) {
		this.listener = listener;
	}
}
