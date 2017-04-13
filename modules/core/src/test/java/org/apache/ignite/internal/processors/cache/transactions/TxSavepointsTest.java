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

package org.apache.ignite.internal.processors.cache.transactions;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

/**
 *
 */
public abstract class TxSavepointsTest extends GridCommonAbstractTest {

	/** {@inheritDoc} */
	@Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
		IgniteConfiguration cfg = super.getConfiguration(gridName);

		TcpDiscoverySpi disc = new TcpDiscoverySpi();

		disc.setIpFinder(new TcpDiscoveryVmIpFinder(true));

		cfg.setDiscoverySpi(disc);

		return cfg;
	}

    /**
     *
     * @param atomicityMode
     * @param cacheMode
     * @return
     */
    protected CacheConfiguration<Integer, Integer> cacheConfig(CacheAtomicityMode atomicityMode,
                                                             CacheMode cacheMode) {
		CacheConfiguration<Integer, Integer> cfg  = new CacheConfiguration<>();

		cfg.setAtomicityMode(atomicityMode);

		cfg.setCacheMode(cacheMode);

		cfg.setBackups(2);

		return cfg;
	}

    /**
     *
     * @param errMsg Message to show for incorrect result.
     * @param cache Cache, which values will be checked.
     */
	protected abstract void checkResult(String errMsg, IgniteCache<Integer, Integer> cache);

    /**
     *
     * @param cfg Will be used to configure cache.
     * @throws Exception If test fails.
     */
    protected void checkSavepoints(CacheConfiguration<Integer, Integer> cfg) throws Exception {
        startGrid(0);
        startGrid(1);
        startGrid(2);

        IgniteCache<Integer, Integer> cache = grid(0).createCache(cfg);

        for (TransactionConcurrency concurrency : TransactionConcurrency.values())
            for (TransactionIsolation isolation : TransactionIsolation.values()) {
                if (isDebug())
                    info(concurrency + " " + isolation);

                cachePutBeforeTx(cache);

                try (Transaction tx = grid(0).transactions().txStart(concurrency, isolation)) {
                    String errMsg = "Broken rollback to savepoint in " + concurrency + " " + isolation + " transaction.";

                    cacheActionsBeforeSavepoint(cache);

                    tx.savepoint("s1");

                    cachePutAfterSavepoint(cache);

                    tx.rollbackToSavepoint("s1");

                    checkResult(errMsg, cache);

                    cacheActionsBeforeSavepoint(cache);

                    tx.savepoint("s2");

                    cachePutAfterSavepoint(cache);

                    tx.rollbackToSavepoint("s2");

                    checkResult(errMsg, cache);

                    tx.commit();
                }
                String errMsg = "Broken commit after savepoint in " + concurrency + " " + isolation + " transaction.";
                checkResult(errMsg, cache);
            }
    }

    /**
     *
     * @param cfg Will be used to configure cache.
     * @throws Exception If test fails.
     */
    protected void checkSavepointsWithTwoCaches(CacheConfiguration<Integer, Integer> cfg) throws Exception {
        startGrid(0);
        startGrid(1);
        startGrid(2);

        IgniteCache<Integer, Integer> cache1 = grid(0).createCache(cfg.setName("First Cache"));
        IgniteCache<Integer, Integer> cache2 = grid(0).createCache(cfg.setName("Second Cache"));

        for (TransactionConcurrency concurrency : TransactionConcurrency.values())
            for (TransactionIsolation isolation : TransactionIsolation.values()) {
                if (isDebug())
                    info(concurrency + " " + isolation);

                cachePutBeforeTx(cache1);
                cachePutBeforeTx(cache2);

                try (Transaction tx = grid(0).transactions().txStart(concurrency, isolation)) {
                    String errMsg = "Broken rollback to savepoint in " + concurrency + " " + isolation + " transaction in cache";

                    cacheActionsBeforeSavepoint(cache1);
                    cacheActionsBeforeSavepoint(cache2);

                    tx.savepoint("s1");

                    cachePutAfterSavepoint(cache1);
                    cachePutAfterSavepoint(cache2);

                    tx.rollbackToSavepoint("s1");

                    checkResult(errMsg + "1.", cache1);
                    checkResult(errMsg + "2.", cache2);

                    cacheActionsBeforeSavepoint(cache1);
                    cacheActionsBeforeSavepoint(cache2);

                    tx.savepoint("s2");

                    cachePutAfterSavepoint(cache1);
                    cachePutAfterSavepoint(cache2);

                    tx.rollbackToSavepoint("s2");

                    checkResult(errMsg + "1.", cache1);
                    checkResult(errMsg + "2.", cache2);

                    tx.commit();
                }
                String errMsg = "Broken commit after savepoint in " + concurrency + " " + isolation + " transaction in cache";
                checkResult(errMsg + "1.", cache1);
                checkResult(errMsg + "2.", cache2);
            }
    }

    private void cachePutAfterSavepoint(IgniteCache<Integer, Integer> cache) {
        cache.put(2, 33);
        cache.remove(3);
        cache.put(4, 33);
        cache.remove(4);
        cache.put(6, 33);
        cache.remove(7);
        cache.put(8, 33);
        cache.remove(8);
        cache.put(10, 33);
        cache.remove(11);
        cache.put(12, 33);
        cache.remove(12);
        cache.put(13, 33);
        cache.remove(14);
        cache.put(15, 33);
        cache.remove(15);
    }

    private void cachePutBeforeTx(IgniteCache<Integer, Integer> cache) {
        cache.put(1, 0);
        cache.put(2, 0);
        cache.put(3, 0);
        cache.put(4, 0);
        cache.put(5, 5);
        cache.put(6, 6);
        cache.put(7, 7);
        cache.put(8, 8);
        cache.remove(9);
        cache.remove(10);
        cache.remove(11);
        cache.remove(12);
        cache.remove(13);
        cache.remove(14);
        cache.remove(15);
    }

    private void cacheActionsBeforeSavepoint(IgniteCache<Integer, Integer> cache) {
        cache.put(1, 1);
        cache.put(2, 2);
        cache.put(3, 3);
        cache.put(4, 4);
        cache.put(9, 9);
        cache.put(10, 10);
        cache.put(11, 11);
        cache.put(12, 12);
    }

    /** {@inheritDoc} */
	@Override protected void afterTest() throws Exception {
		stopAllGrids();
	}
}