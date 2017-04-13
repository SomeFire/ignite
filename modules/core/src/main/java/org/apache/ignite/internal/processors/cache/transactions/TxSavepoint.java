package org.apache.ignite.internal.processors.cache.transactions;

/**
 *
 */
public interface TxSavepoint {

    /**
     * @return Savepoint ID.
     */
	public String getName();

}