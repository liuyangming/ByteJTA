/**
 * Copyright 2014-2015 yangming.liu<liuyangming@gmail.com>.
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
package org.bytesoft.bytejta.logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.bytesoft.bytejta.common.TransactionBeanFactory;
import org.bytesoft.bytejta.logger.store.SimpleTransactionStorageObject;
import org.bytesoft.transaction.archive.TransactionArchive;
import org.bytesoft.transaction.archive.XAResourceArchive;
import org.bytesoft.transaction.logger.TransactionLogger;
import org.bytesoft.transaction.serialize.TransactionArchiveSerializer;
import org.bytesoft.transaction.serialize.XAResourceSerializer;
import org.bytesoft.transaction.store.TransactionStorageKey;
import org.bytesoft.transaction.store.TransactionStorageManager;
import org.bytesoft.transaction.store.TransactionStorageObject;
import org.bytesoft.transaction.xa.AbstractXid;
import org.bytesoft.transaction.xa.XAResourceDescriptor;
import org.bytesoft.transaction.xa.XidFactory;

public class SimpleTransactionLogger implements TransactionLogger, TransactionArchiveSerializer {

	private TransactionStorageManager storageManager;
	private XAResourceSerializer resourceSerializer;

	public void createTransaction(TransactionArchive archive) {
		this.registerResourceIfNecessary(archive);

		byte[] byteArray = this.serialize(archive);
		SimpleTransactionStorageObject storageObject = new SimpleTransactionStorageObject(byteArray);
		this.storageManager.createStorageObject(storageObject);
	}

	public void updateTransaction(TransactionArchive archive) {
		this.registerResourceIfNecessary(archive);

		byte[] byteArray = this.serialize(archive);
		SimpleTransactionStorageObject storageObject = new SimpleTransactionStorageObject(byteArray);
		this.storageManager.modifyStorageObject(storageObject);
	}

	private void registerResourceIfNecessary(TransactionArchive archive) {
		List<XAResourceArchive> nativeResources = archive.getNativeResources();
		for (int i = 0; i < nativeResources.size(); i++) {
			XAResourceArchive resourceArchive = nativeResources.get(i);
			XAResourceDescriptor descriptor = resourceArchive.getDescriptor();
			if (descriptor.isSupportsXA() && descriptor.getDescriptorId() <= 0) {
				String identifier = descriptor.getIdentifier();
				int descriptorId = this.storageManager.registerResource(identifier);
				descriptor.setDescriptorId(descriptorId);
			} else if (descriptor.isSupportsXA() == false && descriptor.getDescriptorId() == 0) {
				descriptor.setDescriptorId(-1);
			}
		}

		List<XAResourceArchive> remoteResources = archive.getRemoteResources();
		for (int i = 0; i < remoteResources.size(); i++) {
			XAResourceArchive resourceArchive = remoteResources.get(i);
			XAResourceDescriptor descriptor = resourceArchive.getDescriptor();
			if (descriptor.isSupportsXA() && descriptor.getDescriptorId() <= 0) {
				String identifier = descriptor.getIdentifier();
				int descriptorId = this.storageManager.registerResource(identifier);
				descriptor.setDescriptorId(descriptorId);
			} else if (descriptor.isSupportsXA() == false && descriptor.getDescriptorId() == 0) {
				descriptor.setDescriptorId(-1);
			}
		}
	}

	public void deleteTransaction(TransactionArchive archive) {
		byte[] byteArray = this.serialize(archive);
		SimpleTransactionStorageObject storageObject = new SimpleTransactionStorageObject(byteArray);
		this.storageManager.deleteStorageObject(storageObject);
	}

	public List<TransactionArchive> getTransactionArchiveList() throws IllegalStateException {
		List<TransactionStorageKey> keyList = this.storageManager.getStorageKeyList();
		List<TransactionArchive> archives = new ArrayList<TransactionArchive>();
		for (int i = 0; i < keyList.size(); i++) {
			TransactionStorageKey storageKey = keyList.get(i);
			TransactionStorageObject storageObject = this.storageManager.locateStorageObject(storageKey);
			byte[] byteArray = storageObject.getContentByteArray();
			TransactionArchive archive = null;
			try {
				archive = this.deserialize(byteArray);
				archives.add(archive);
			} catch (IOException ex) {
				throw new IllegalStateException(ex);
			}

		}
		return archives;
	}

	public void updateResource(Xid transactionXid, XAResourceArchive archive) {
		// Not support yet.
	}

	public byte[] serialize(TransactionArchive archive) {
		ByteBuffer buffer = ByteBuffer.allocate(4096);
		Xid xid = archive.getXid();
		byte[] transactionId = xid.getGlobalTransactionId();
		buffer.put(transactionId);
		int status = archive.getStatus();
		buffer.put((byte) status);
		int vote = archive.getVote();
		buffer.put((byte) vote);
		byte compensable = archive.isCompensable() ? (byte) 1 : (byte) 0;
		buffer.put((byte) compensable);
		byte coordinator = archive.isCoordinator() ? (byte) 1 : (byte) 0;
		buffer.put((byte) coordinator);
		byte optimized = archive.isOptimized() ? (byte) 1 : (byte) 0;
		buffer.put((byte) optimized);

		List<XAResourceArchive> nativeResources = archive.getNativeResources();
		List<XAResourceArchive> remoteResources = archive.getRemoteResources();
		int nativeNumber = nativeResources.size();
		int remoteNumber = remoteResources.size();
		buffer.put((byte) nativeNumber);
		buffer.put((byte) remoteNumber);

		for (int i = 0; i < nativeNumber; i++) {
			XAResourceArchive resourceArchive = nativeResources.get(i);
			this.serializeXAResourceArchive(buffer, resourceArchive);
		}

		for (int i = 0; i < remoteNumber; i++) {
			XAResourceArchive resourceArchive = remoteResources.get(i);
			this.serializeXAResourceArchive(buffer, resourceArchive);
		}

		int pos = buffer.position();
		byte[] byteArray = new byte[pos];
		buffer.flip();
		buffer.get(byteArray);

		return byteArray;
	}

	private void serializeXAResourceArchive(ByteBuffer buffer, XAResourceArchive resourceArchive) {
		XAResourceDescriptor descriptor = resourceArchive.getDescriptor();
		byte descriptorId = (byte) descriptor.getDescriptorId();
		Xid branchXid = resourceArchive.getXid();

		byte[] branchQualifier = branchXid.getBranchQualifier();
		byte branchVote = (byte) resourceArchive.getVote();
		byte rolledback = resourceArchive.isRolledback() ? (byte) 1 : (byte) 0;
		byte committed = resourceArchive.isCommitted() ? (byte) 1 : (byte) 0;
		byte heuristic = resourceArchive.isHeuristic() ? (byte) 1 : (byte) 0;

		buffer.put(branchQualifier);
		buffer.put(descriptorId);
		buffer.put(branchVote);
		buffer.put(committed);
		buffer.put(rolledback);
		buffer.put(heuristic);
	}

	public TransactionArchive deserialize(byte[] byteArray) throws IOException {
		ByteBuffer buffer = ByteBuffer.wrap(byteArray);

		TransactionArchive archive = new TransactionArchive();
		byte[] transactionId = new byte[XidFactory.GLOBAL_TRANSACTION_LENGTH];
		buffer.get(transactionId);

		XidFactory xidFactory = TransactionBeanFactory.getInstance().getXidFactory();
		AbstractXid globalXid = xidFactory.createGlobalXid(transactionId);
		archive.setXid(globalXid);

		int status = buffer.get();
		int vote = buffer.get();
		int compensableValue = buffer.get();
		int coordinatorValue = buffer.get();
		int optimizedValue = buffer.get();

		archive.setStatus(status);
		archive.setVote(vote);
		archive.setCompensable(compensableValue != 0);
		archive.setCoordinator(coordinatorValue != 0);
		archive.setOptimized(optimizedValue != 0);

		int nativeNumber = buffer.get();
		int remoteNumber = buffer.get();
		for (int i = 0; i < nativeNumber; i++) {
			XAResourceArchive resourceArchive = null;
			resourceArchive = this.deserializeXAResourceArchive(globalXid, buffer);
			archive.getNativeResources().add(resourceArchive);
		}
		for (int i = 0; i < remoteNumber; i++) {
			XAResourceArchive resourceArchive = null;
			resourceArchive = this.deserializeXAResourceArchive(globalXid, buffer);
			archive.getRemoteResources().add(resourceArchive);
		}
		return archive;
	}

	private XAResourceArchive deserializeXAResourceArchive(AbstractXid globalXid, ByteBuffer buffer) throws IOException {
		byte[] branchQualifier = new byte[XidFactory.BRANCH_QUALIFIER_LENGTH];
		buffer.get(branchQualifier);
		int descriptorId = buffer.get();
		int branchVote = buffer.get();
		int committedValue = buffer.get();
		int rolledbackValue = buffer.get();
		int heuristicValue = buffer.get();
		XAResourceArchive resourceArchive = new XAResourceArchive();
		XidFactory xidFactory = TransactionBeanFactory.getInstance().getXidFactory();
		AbstractXid branchXid = xidFactory.createBranchXid(globalXid, branchQualifier);
		resourceArchive.setXid(branchXid);
		resourceArchive.setVote(branchVote);
		resourceArchive.setRolledback(rolledbackValue != 0);
		resourceArchive.setCommitted(committedValue != 0);
		resourceArchive.setHeuristic(heuristicValue != 0);
		if (branchVote == XAResource.XA_RDONLY) {
			resourceArchive.setCompleted(true);
		} else if (resourceArchive.isCommitted() || resourceArchive.isRolledback()) {
			resourceArchive.setCompleted(true);
		}

		if (descriptorId >= 0) {
			String identifier = this.storageManager.getRegisteredResource(descriptorId);
			XAResourceDescriptor descriptor = this.resourceSerializer.deserialize(identifier);
			descriptor.setDescriptorId(descriptorId);
			resourceArchive.setDescriptor(descriptor);
		} else {
			XAResourceDescriptor descriptor = new XAResourceDescriptor();
			descriptor.setRemote(false);
			descriptor.setSupportsXA(false);
			descriptor.setDescriptorId(descriptorId);
			resourceArchive.setDescriptor(descriptor);
		}

		return resourceArchive;

	}

	public TransactionStorageManager getStorageManager() {
		return storageManager;
	}

	public void setStorageManager(TransactionStorageManager storageManager) {
		this.storageManager = storageManager;
	}

	public XAResourceSerializer getResourceSerializer() {
		return resourceSerializer;
	}

	public void setResourceSerializer(XAResourceSerializer resourceSerializer) {
		this.resourceSerializer = resourceSerializer;
	}

}
