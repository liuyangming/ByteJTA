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
package org.bytesoft.transaction;

public interface TransactionListener {

	public int OPT_DEFAULT = 0;
	public int OPT_HEURCOM = 1;
	public int OPT_HEURRB = 2;
	public int OPT_HEURMIX = 3;

	public void prepareStart();

	public void prepareComplete(boolean success);

	public void commitStart();

	public void commitSuccess();

	public void commitFailure(int optcode);

	public void rollbackStart();

	public void rollbackSuccess();

	public void rollbackFailure(int optcode);

}
