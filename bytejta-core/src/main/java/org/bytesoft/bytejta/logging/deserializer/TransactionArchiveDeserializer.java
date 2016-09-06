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

import org.bytesoft.transaction.archive.TransactionArchive;
import org.bytesoft.transaction.archive.XAResourceArchive;
import org.bytesoft.transaction.logging.ArchiveDeserializer;
import org.bytesoft.transaction.xa.TransactionXid;

public class TransactionArchiveDeserializer implements ArchiveDeserializer {

	private ArchiveDeserializer resourceArchiveDeserializer;

	public byte[] serialize(TransactionXid xid, Object obj) {
		TransactionArchive archive = (TransactionArchive) obj;

		ByteBuffer buffer = ByteBuffer.allocate(8192);
		int status = archive.getStatus();
		buffer.put((byte) status);
		int vote = archive.getVote();
		buffer.put((byte) vote);
		byte coordinator = archive.isCoordinator() ? (byte) 0x1 : (byte) 0x0;
		buffer.put((byte) coordinator);

		List<XAResourceArchive> nativeResources = archive.getNativeResources();
		List<XAResourceArchive> remoteResources = archive.getRemoteResources();
		int nativeNumber = nativeResources.size();
		int remoteNumber = remoteResources.size();
		buffer.put((byte) nativeNumber);
		buffer.put((byte) remoteNumber);

		for (int i = 0; i < nativeNumber; i++) {
			XAResourceArchive resourceArchive = nativeResources.get(i);

			byte[] resourceByteArray = this.resourceArchiveDeserializer.serialize(xid, resourceArchive);

			buffer.put((byte) resourceByteArray.length);
			buffer.put(resourceByteArray);
		}

		for (int i = 0; i < remoteNumber; i++) {
			XAResourceArchive resourceArchive = remoteResources.get(i);

			byte[] resourceByteArray = this.resourceArchiveDeserializer.serialize(xid, resourceArchive);

			buffer.put((byte) resourceByteArray.length);
			buffer.put(resourceByteArray);
		}

		int pos = buffer.position();
		byte[] byteArray = new byte[pos];
		buffer.flip();
		buffer.get(byteArray);

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

		int nativeNumber = buffer.get();
		int remoteNumber = buffer.get();
		for (int i = 0; i < nativeNumber; i++) {
			int length = buffer.get();
			byte[] resourceByteArray = new byte[length];
			buffer.get(resourceByteArray);

			XAResourceArchive resourceArchive = //
			(XAResourceArchive) this.resourceArchiveDeserializer.deserialize(xid, resourceByteArray);

			archive.getNativeResources().add(resourceArchive);
		}
		for (int i = 0; i < remoteNumber; i++) {
			int length = buffer.get();
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
