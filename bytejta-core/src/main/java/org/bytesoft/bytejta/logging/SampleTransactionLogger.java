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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.bytesoft.bytejta.logging.store.VirtualLoggingSystemImpl;
import org.bytesoft.bytejta.supports.resource.CommonResourceDescriptor;
import org.bytesoft.bytejta.supports.resource.RemoteResourceDescriptor;
import org.bytesoft.bytejta.supports.resource.UnidentifiedResourceDescriptor;
import org.bytesoft.bytejta.supports.wire.RemoteCoordinator;
import org.bytesoft.transaction.TransactionBeanFactory;
import org.bytesoft.transaction.archive.TransactionArchive;
import org.bytesoft.transaction.archive.XAResourceArchive;
import org.bytesoft.transaction.aware.TransactionBeanFactoryAware;
import org.bytesoft.transaction.logging.TransactionLogger;
import org.bytesoft.transaction.recovery.TransactionRecoveryCallback;
import org.bytesoft.transaction.supports.resource.XAResourceDescriptor;
import org.bytesoft.transaction.supports.serialize.XAResourceDeserializer;
import org.bytesoft.transaction.xa.TransactionXid;
import org.bytesoft.transaction.xa.XidFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SampleTransactionLogger extends VirtualLoggingSystemImpl implements TransactionLogger, TransactionBeanFactoryAware {
	static final Logger logger = LoggerFactory.getLogger(SampleTransactionLogger.class.getSimpleName());

	static final int TYPE_TRANSACTION = 0x0;
	static final int TYPE_XARESOURCE = 0x1;

	private TransactionBeanFactory beanFactory;
	private XAResourceDeserializer deserializer;

	public void createTransaction(TransactionArchive archive) {
		byte[] values = this.serialize(archive);

		byte[] byteArray = new byte[values.length + 1];
		byteArray[0] = TYPE_TRANSACTION;
		System.arraycopy(values, 0, byteArray, 1, values.length);

		this.create(archive.getXid(), byteArray);
	}

	public void updateTransaction(TransactionArchive archive) {
		byte[] values = this.serialize(archive);

		byte[] byteArray = new byte[values.length + 1];
		byteArray[0] = TYPE_TRANSACTION;
		System.arraycopy(values, 0, byteArray, 1, values.length);

		this.modify(archive.getXid(), byteArray);
	}

	public void deleteTransaction(TransactionArchive archive) {
		this.delete(archive.getXid());
	}

	public void recover(TransactionRecoveryCallback callback) {
		// TODO
	}

	public void updateResource(XAResourceArchive archive) {
		byte[] values = this.serializeResource(archive);

		byte[] byteArray = new byte[values.length + 1];
		byteArray[0] = TYPE_XARESOURCE;
		System.arraycopy(values, 0, byteArray, 1, values.length);

		this.modify(archive.getXid(), byteArray);
	}

	public byte[] serialize(TransactionArchive archive) {

		ByteBuffer buffer = ByteBuffer.allocate(8192);
		int status = archive.getStatus();
		buffer.put((byte) status);
		int vote = archive.getVote();
		buffer.put((byte) vote);
		byte coordinator = archive.isCoordinator() ? (byte) 1 : (byte) 0;
		buffer.put((byte) coordinator);

		List<XAResourceArchive> nativeResources = archive.getNativeResources();
		List<XAResourceArchive> remoteResources = archive.getRemoteResources();
		int nativeNumber = nativeResources.size();
		int remoteNumber = remoteResources.size();
		buffer.put((byte) nativeNumber);
		buffer.put((byte) remoteNumber);

		for (int i = 0; i < nativeNumber; i++) {
			XAResourceArchive resourceArchive = nativeResources.get(i);

			byte[] resourceByteArray = this.serializeResource(resourceArchive);

			buffer.put((byte) resourceByteArray.length);
			buffer.put(resourceByteArray);
		}

		for (int i = 0; i < remoteNumber; i++) {
			XAResourceArchive resourceArchive = remoteResources.get(i);

			byte[] resourceByteArray = this.serializeResource(resourceArchive);

			buffer.put((byte) resourceByteArray.length);
			buffer.put(resourceByteArray);
		}

		int pos = buffer.position();
		byte[] byteArray = new byte[pos];
		buffer.flip();
		buffer.get(byteArray);

		return byteArray;
	}

	public TransactionArchive deserialize(TransactionXid globalXid, byte[] byteArray) throws IOException {
		ByteBuffer buffer = ByteBuffer.wrap(byteArray);

		TransactionArchive archive = new TransactionArchive();
		archive.setXid(globalXid);

		int status = buffer.get();
		int vote = buffer.get();
		int coordinatorValue = buffer.get();

		archive.setStatus(status);
		archive.setVote(vote);
		archive.setCoordinator(coordinatorValue != 0);

		int nativeNumber = buffer.get();
		int remoteNumber = buffer.get();
		for (int i = 0; i < nativeNumber; i++) {
			int length = buffer.get();
			byte[] resourceByteArray = new byte[length];
			buffer.get(resourceByteArray);

			XAResourceArchive resourceArchive = this.deserializeResource(globalXid, resourceByteArray);
			archive.getNativeResources().add(resourceArchive);
		}
		for (int i = 0; i < remoteNumber; i++) {
			int length = buffer.get();
			byte[] resourceByteArray = new byte[length];
			buffer.get(resourceByteArray);

			XAResourceArchive resourceArchive = this.deserializeResource(globalXid, resourceByteArray);
			archive.getRemoteResources().add(resourceArchive);
		}
		return archive;
	}

	private byte[] serializeResource(XAResourceArchive resourceArchive) {

		ByteBuffer buffer = ByteBuffer.allocate(8192);

		Xid branchXid = resourceArchive.getXid();
		byte[] branchQualifier = branchXid.getBranchQualifier();
		buffer.put(branchQualifier);

		XAResourceDescriptor descriptor = resourceArchive.getDescriptor();
		if (CommonResourceDescriptor.class.isInstance(descriptor)) {
			String identifier = descriptor.getIdentifier();
			byte[] identifierByteArray = identifier.getBytes();
			buffer.put((byte) 0x1);
			buffer.put((byte) identifierByteArray.length);
			buffer.put(identifierByteArray);
		} else if (RemoteResourceDescriptor.class.isInstance(descriptor)) {
			String identifier = descriptor.getIdentifier();
			byte[] identifierByteArray = identifier.getBytes();
			buffer.put((byte) 0x2);
			buffer.put((byte) identifierByteArray.length);
			buffer.put(identifierByteArray);
		} else {
			buffer.put((byte) 0x0);
			buffer.put((byte) 0x0);
		}

		byte branchVote = (byte) resourceArchive.getVote();
		byte readonly = resourceArchive.isReadonly() ? (byte) 1 : (byte) 0;
		byte committed = resourceArchive.isCommitted() ? (byte) 1 : (byte) 0;
		byte rolledback = resourceArchive.isRolledback() ? (byte) 1 : (byte) 0;
		byte completed = resourceArchive.isCompleted() ? (byte) 1 : (byte) 0;
		byte heuristic = resourceArchive.isHeuristic() ? (byte) 1 : (byte) 0;

		buffer.put(branchVote);
		buffer.put(readonly);
		buffer.put(committed);
		buffer.put(rolledback);
		buffer.put(completed);
		buffer.put(heuristic);

		int position = buffer.position();
		byte[] byteArray = new byte[position];
		buffer.flip();
		buffer.get(byteArray);

		return byteArray;
	}

	private XAResourceArchive deserializeResource(TransactionXid globalXid, byte[] array) throws IOException {

		ByteBuffer buffer = ByteBuffer.wrap(array);

		XAResourceArchive archive = new XAResourceArchive();

		byte[] branchQualifier = new byte[XidFactory.BRANCH_QUALIFIER_LENGTH];
		buffer.get(branchQualifier);
		XidFactory xidFactory = this.beanFactory.getXidFactory();
		TransactionXid branchXid = xidFactory.createBranchXid(globalXid, branchQualifier);
		archive.setXid(branchXid);

		XAResourceDescriptor descriptor = null;
		byte resourceType = buffer.get();
		byte length = buffer.get();
		byte[] byteArray = new byte[length];
		buffer.get(byteArray);
		String identifier = new String(byteArray);

		if (resourceType == 0x01) {
			archive.setIdentified(true);
			CommonResourceDescriptor resourceDescriptor = new CommonResourceDescriptor();
			XAResource resource = this.deserializer.deserialize(identifier);
			resourceDescriptor.setDelegate(resource);
			descriptor = resourceDescriptor;
		} else if (resourceType == 0x02) {
			archive.setIdentified(true);
			RemoteResourceDescriptor resourceDescriptor = new RemoteResourceDescriptor();
			XAResource resource = this.deserializer.deserialize(identifier);
			resourceDescriptor.setDelegate((RemoteCoordinator) resource);
			descriptor = resourceDescriptor;
			descriptor = resourceDescriptor;
		} else {
			UnidentifiedResourceDescriptor resourceDescriptor = new UnidentifiedResourceDescriptor();
			descriptor = resourceDescriptor;
		}
		archive.setDescriptor(descriptor);

		int branchVote = buffer.get();
		int readonly = buffer.get();
		int committedValue = buffer.get();
		int rolledbackValue = buffer.get();
		int completedValue = buffer.get();
		int heuristicValue = buffer.get();
		archive.setVote(branchVote);
		archive.setReadonly(readonly != 0);
		archive.setCommitted(committedValue != 0);
		archive.setRolledback(rolledbackValue != 0);
		archive.setCompleted(completedValue != 0);
		archive.setHeuristic(heuristicValue != 0);

		return archive;

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

	public XAResourceDeserializer getDeserializer() {
		return deserializer;
	}

	public void setDeserializer(XAResourceDeserializer deserializer) {
		this.deserializer = deserializer;
	}

}
