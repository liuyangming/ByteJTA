/**
 * Copyright 2014 yangming.liu<liuyangming@gmail.com>.
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
package org.bytesoft.transaction.xa;

public interface XidFactory {
	public static final int JTA_FORMAT_ID = 1207;
	public static final int TCC_FORMAT_ID = 8127;

	public static final int GLOBAL_TRANSACTION_LENGTH = 32;
	public static final int BRANCH_QUALIFIER_LENGTH = 32;

	public int getFormatId();

	public TransactionXid createGlobalXid();

	public TransactionXid createGlobalXid(byte[] globalTransactionId);

	public TransactionXid createBranchXid(TransactionXid globalXid);

	public TransactionXid createBranchXid(TransactionXid globalXid, byte[] branchQualifier);
}
