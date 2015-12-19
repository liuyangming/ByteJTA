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
package org.bytesoft.transaction.xa;

import java.util.List;

import javax.transaction.RollbackException;
import javax.transaction.SystemException;

import org.bytesoft.transaction.archive.XAResourceArchive;

public interface XATerminator extends javax.transaction.xa.XAResource {

	public boolean delistResource(XAResourceDescriptor xaRes, int flag) throws IllegalStateException, SystemException;

	public boolean enlistResource(XAResourceDescriptor xaRes) throws RollbackException, IllegalStateException, SystemException;

	public void resumeAllResource() throws RollbackException, SystemException;

	public void suspendAllResource() throws RollbackException, SystemException;

	public void delistAllResource() throws RollbackException, SystemException;

	public List<XAResourceArchive> getResourceArchives();

}
