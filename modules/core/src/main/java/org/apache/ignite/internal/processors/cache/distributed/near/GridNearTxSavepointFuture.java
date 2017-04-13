package org.apache.ignite.internal.processors.cache.distributed.near;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxEntry;
import org.apache.ignite.internal.transactions.IgniteTxHeuristicCheckedException;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.transactions.TransactionRollbackException;

import static org.apache.ignite.internal.processors.cache.GridCacheOperation.NOOP;

public class GridNearTxSavepointFuture<K, V> extends GridNearTxFinishFuture<K, V> {

	private String name;

	public GridNearTxSavepointFuture(GridCacheSharedContext<K, V> cctx, GridNearTxLocal tx, String name) {
		super(cctx, tx, false);
		this.name = name;
	}

	public void savepoint() {
		tx().savepoint(name);

		onDone();
	}

	public void rollbackToSavepoint() {
		tx().rollbackToSavepoint(name);

		onDone();
	}

	public void releaseCheckpoint() {
		tx().releaseCheckpoint(name);

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
				tx().setRollbackOnly();

			if (onDoneForSP(tx0, err)) {

				// Don't forget to clean up.
				cctx.mvcc().removeFuture(futureId());

				return true;
			}
		}

		return false;
	}
}