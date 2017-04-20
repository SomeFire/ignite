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
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;

/**
 *
 */
public class TxSavepointsSelfTest extends GridCommonAbstractTest {

    private static String ERR_MSG = "No such savepoint.";

    /** */
    private IgniteCache<Integer, Integer> cache;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi disc = new TcpDiscoverySpi();

        disc.setIpFinder(new TcpDiscoveryVmIpFinder(true));

        cfg.setDiscoverySpi(disc);

        cfg.setCacheConfiguration(new CacheConfiguration().setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        startGrid(0);

        cache = grid(0).cache(null);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * Tests savepoint.
     */
    public void testSavepoints() {
        try (Transaction tx = grid(0).transactions().txStart()) {
            putThreeValuesAndCreateSavepoints(tx);

            tx.rollbackToSavepoint("s2");

            tx.commit();
        }
        assertEquals((Integer) 2, cache.get(2));
    }

    /**
     * Tests valid and invalid rollbacks to savepoint.
     */
    public void testFailRollbackToSavepoint() {
        Exception err = null;
        try (Transaction tx = grid(0).transactions().txStart()) {
            putThreeValuesAndCreateSavepoints(tx);

            tx.rollbackToSavepoint("s2");

            assertEquals((Integer) 2, cache.get(2));

            tx.rollbackToSavepoint("s3");
        } catch (Exception e) {
            assertTrue("Unexpected exception: " + e.getMessage(), e.getMessage().startsWith(ERR_MSG));

            err = e;
        }
        assertNotNull(err);
    }

    /**
     * Tests savepoint deleting.
     */
    public void testReleaseSavepoints() {
        try (Transaction tx = grid(0).transactions().txStart()) {
            putThreeValuesAndCreateSavepoints(tx);

            tx.releaseSavepoint("s1");

            tx.releaseSavepoint("s3");

            tx.rollbackToSavepoint("s2");

            tx.commit();
        }
        assertEquals((Integer) 2, cache.get(2));
    }

    /**
     * Tests rollbacks to the same savepoint instance.
     */
    public void testMultipleRollbackToSavepoint() {
        try (Transaction tx = grid(0).transactions().txStart()) {
            putThreeValuesAndCreateSavepoints(tx);

            tx.rollbackToSavepoint("s2");

            cache.put(2, 3);

            tx.rollbackToSavepoint("s2");

            tx.commit();
        }
        assertEquals((Integer) 2, cache.get(2));
    }

    /**
     * Tests savepoints in failed transaction.
     */
    public void testTransactionRollback() {
        try (Transaction tx = grid(0).transactions().txStart()) {
            putThreeValuesAndCreateSavepoints(tx);

            tx.releaseSavepoint("s3");

            tx.rollbackToSavepoint("s2");

            tx.rollback();
        }
        assertEquals(null, cache.get(2));
    }

    /**
     * Tests two caches with different atomicity.
     */
    public void testMultiCaches() {
        IgniteCache<Integer, Integer> cache1 = grid(0)
                .createCache(new CacheConfiguration<Integer, Integer>(cache.getConfiguration(CacheConfiguration.class))
                        .setAtomicityMode(ATOMIC)
                        .setName("Second Cache"));
        try (Transaction tx = grid(0).transactions().txStart()) {
            cache1.put(2, 1);

            putThreeValuesAndCreateSavepoints(tx);

            cache1.put(2, 2);

            tx.rollbackToSavepoint("s2");

            tx.commit();
        }
        assertEquals((Integer) 2, cache.get(2));
        assertEquals((Integer) 2, cache1.get(2));
    }

    /**
     * @param tx Transaction for savepoints.
     */
    private void putThreeValuesAndCreateSavepoints(Transaction tx) {
        cache.put(2, 1);
        tx.savepoint("s1");
        cache.put(2, 2);
        tx.savepoint("s2");
        cache.put(2, 3);
        tx.savepoint("s3");
    }

}