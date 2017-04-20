/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.distributed.near;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.internal.processors.cache.GridCacheFuture;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.util.future.GridCompoundIdentityFuture;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class GridNearTxSavepointFuture<K, V> extends GridCompoundIdentityFuture<IgniteInternalTx>
		implements GridCacheFuture<IgniteInternalTx> {

    /** */
    private static final long serialVersionUID = 0L;

    /** Logger reference. */
    private static final AtomicReference<IgniteLogger> logRef = new AtomicReference<>();

    /** Logger. */
    private static IgniteLogger log;

    /** Logger. */
    protected static IgniteLogger msgLog;

    /** Context. */
    private GridCacheSharedContext<K, V> cctx;

    /** Future ID. */
    private final IgniteUuid futId;

    /** Transaction. */
    @GridToStringInclude
    private GridNearTxLocal tx;

    /** Trackable flag. */
    private boolean trackable = true;

    /** Savepoint ID*/
	private String name;

    /**
     * @param cctx Context.
     * @param tx Transaction.
     * @param name Savepoint ID.
     */
	public GridNearTxSavepointFuture(GridCacheSharedContext<K, V> cctx, GridNearTxLocal tx, String name) {
        super(F.<IgniteInternalTx>identityReducer(tx));

        this.cctx = cctx;
        this.tx = tx;

        futId = IgniteUuid.randomUuid();

        if (log == null) {
            msgLog = cctx.messageLogger();
            log = U.logger(cctx.kernalContext(), logRef, GridNearTxSavepointFuture.class);
        }
		this.name = name;
	}

    /**
     * Calls creating savepoint for transaction.
     */
	public void savepoint() {
		tx.savepoint(name);

		onDone();
	}

    /**
     * Calls rollback to savepoint for transaction.
     */
	public void rollbackToSavepoint() {
		tx.rollbackToSavepoint(name);

		onDone();
	}

    /**
     * Calls deleting of savepoint for transaction.
     */
	public void releaseSavepoint() {
		tx.releaseSavepoint(name);

		onDone();
	}

	/** {@inheritDoc} */
	@Override public boolean onDone(IgniteInternalTx tx0, Throwable err) {
		if (isDone())
			return false;

		synchronized (this) {
			if (isDone())
				return false;

			if (err != null)
				tx.setRollbackOnly();

			if (super.onDone(tx0, err)) {

				// Don't forget to clean up.
				cctx.mvcc().removeFuture(futureId());

				return true;
			}
		}

		return false;
	}


    /** {@inheritDoc} */
    @Override public IgniteUuid futureId() {
        return futId;
    }

    /** {@inheritDoc} */
    @Override public boolean onNodeLeft(UUID nodeId) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean trackable() {
        return trackable;
    }

    /** {@inheritDoc} */
    @Override public void markNotTrackable() {
        trackable = false;
    }
}