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
package org.bytesoft.bytejta.logging.deserializer;

import java.nio.ByteBuffer;
import java.util.List;

import org.bytesoft.common.utils.ByteUtils;
import org.bytesoft.transaction.archive.TransactionArchive;
import org.bytesoft.transaction.archive.XAResourceArchive;
import org.bytesoft.transaction.logging.ArchiveDeserializer;
import org.bytesoft.transaction.xa.TransactionXid;

public class TransactionArchiveDeserializer implements ArchiveDeserializer {

	private ArchiveDeserializer resourceArchiveDeserializer;

	public byte[] serialize(TransactionXid xid, Object obj) {
		TransactionArchive archive = (TransactionArchive) obj;

		XAResourceArchive optimizedArchive = archive.getOptimizedResource();

		List<XAResourceArchive> nativeArchiveList = archive.getNativeResources();
		List<XAResourceArchive> remoteArchiveList = archive.getRemoteResources();

		int optimizedArchiveNumber = optimizedArchive == null ? 0 : 1;

		int nativeArchiveNumber = nativeArchiveList.size();
		int remoteArchiveNumber = remoteArchiveList.size();

		int transactionStrategy = archive.getTransactionStrategyType();

		int length = 3 + 3 + 1;
		byte[][] nativeByteArray = new byte[nativeArchiveNumber][];
		for (int i = 0; i < nativeArchiveNumber; i++) {
			XAResourceArchive resourceArchive = nativeArchiveList.get(i);

			byte[] resourceByteArray = this.resourceArchiveDeserializer.serialize(xid, resourceArchive);
			byte[] lengthByteArray = ByteUtils.shortToByteArray((short) resourceByteArray.length);

			byte[] elementByteArray = new byte[resourceByteArray.length + 2];
			System.arraycopy(lengthByteArray, 0, elementByteArray, 0, lengthByteArray.length);
			System.arraycopy(resourceByteArray, 0, elementByteArray, 2, resourceByteArray.length);

			nativeByteArray[i] = elementByteArray;
			length = length + elementByteArray.length;
		}

		byte[] optimizedByteArray = new byte[0];
		if (optimizedArchiveNumber > 0) {
			byte[] resourceByteArray = this.resourceArchiveDeserializer.serialize(xid, optimizedArchive);
			byte[] lengthByteArray = ByteUtils.shortToByteArray((short) resourceByteArray.length);

			byte[] elementByteArray = new byte[resourceByteArray.length + 2];
			System.arraycopy(lengthByteArray, 0, elementByteArray, 0, lengthByteArray.length);
			System.arraycopy(resourceByteArray, 0, elementByteArray, 2, resourceByteArray.length);

			optimizedByteArray = elementByteArray;
			length = length + elementByteArray.length;
		}

		byte[][] remoteByteArray = new byte[remoteArchiveNumber][];
		for (int i = 0; i < remoteArchiveNumber; i++) {
			XAResourceArchive resourceArchive = remoteArchiveList.get(i);

			byte[] resourceByteArray = this.resourceArchiveDeserializer.serialize(xid, resourceArchive);
			byte[] lengthByteArray = ByteUtils.shortToByteArray((short) resourceByteArray.length);

			byte[] elementByteArray = new byte[resourceByteArray.length + 2];
			System.arraycopy(lengthByteArray, 0, elementByteArray, 0, lengthByteArray.length);
			System.arraycopy(resourceByteArray, 0, elementByteArray, 2, resourceByteArray.length);

			remoteByteArray[i] = elementByteArray;
			length = length + elementByteArray.length;
		}

		int position = 0;

		byte[] byteArray = new byte[length];
		byteArray[position++] = (byte) archive.getStatus();
		byteArray[position++] = (byte) archive.getVote();
		byteArray[position++] = archive.isCoordinator() ? (byte) 0x1 : (byte) 0x0;

		byteArray[position++] = (byte) nativeArchiveNumber;
		byteArray[position++] = (byte) optimizedArchiveNumber;
		byteArray[position++] = (byte) remoteArchiveNumber;

		byteArray[position++] = (byte) transactionStrategy;

		for (int i = 0; i < nativeArchiveNumber; i++) {
			byte[] elementByteArray = nativeByteArray[i];
			System.arraycopy(elementByteArray, 0, byteArray, position, elementByteArray.length);
			position = position + elementByteArray.length;
		}

		if (optimizedArchiveNumber > 0) {
			System.arraycopy(optimizedByteArray, 0, byteArray, position, optimizedByteArray.length);
			position = position + optimizedByteArray.length;
		}

		for (int i = 0; i < remoteArchiveNumber; i++) {
			byte[] elementByteArray = remoteByteArray[i];
			System.arraycopy(elementByteArray, 0, byteArray, position, elementByteArray.length);
			position = position + elementByteArray.length;
		}

		return byteArray;
	}

	public Object deserialize(TransactionXid xid, byte[] array) {

		ByteBuffer buffer = ByteBuffer.wrap(array);

		TransactionArchive archive = new TransactionArchive();
		archive.setXid(xid);

		int status = buffer.get();
		int vote = buffer.get();
		int coordinatorValue = buffer.get();

		archive.setStatus(status);
		archive.setVote(vote);
		archive.setCoordinator(coordinatorValue != 0);

		int nativeArchiveNumber = buffer.get();
		int optimizedArchiveNumber = buffer.get();
		int remoteArchiveNumber = buffer.get();

		int transactionStrategyType = buffer.get();
		archive.setTransactionStrategyType(transactionStrategyType);

		for (int i = 0; i < nativeArchiveNumber; i++) {
			int length = buffer.getShort();
			byte[] resourceByteArray = new byte[length];
			buffer.get(resourceByteArray);

			XAResourceArchive resourceArchive = //
					(XAResourceArchive) this.resourceArchiveDeserializer.deserialize(xid, resourceByteArray);

			archive.getNativeResources().add(resourceArchive);
		}

		if (optimizedArchiveNumber > 0) {
			int length = buffer.getShort();
			byte[] resourceByteArray = new byte[length];
			buffer.get(resourceByteArray);

			XAResourceArchive resourceArchive = //
					(XAResourceArchive) this.resourceArchiveDeserializer.deserialize(xid, resourceByteArray);

			archive.setOptimizedResource(resourceArchive);
		}

		for (int i = 0; i < remoteArchiveNumber; i++) {
			int length = buffer.getShort();
			byte[] resourceByteArray = new byte[length];
			buffer.get(resourceByteArray);

			XAResourceArchive resourceArchive = //
					(XAResourceArchive) this.resourceArchiveDeserializer.deserialize(xid, resourceByteArray);

			archive.getRemoteResources().add(resourceArchive);
		}

		return archive;
	}

	public ArchiveDeserializer getResourceArchiveDeserializer() {
		return resourceArchiveDeserializer;
	}

	public void setResourceArchiveDeserializer(ArchiveDeserializer resourceArchiveDeserializer) {
		this.resourceArchiveDeserializer = resourceArchiveDeserializer;
	}

}
