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

package org.apache.ignite.internal.processors.cache;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.swapspace.SwapSpaceSpi;
import org.apache.ignite.spi.swapspace.noop.NoopSwapSpaceSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.configuration.DeploymentMode.SHARED;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_SWAPPED;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_UNSWAPPED;
import static org.apache.ignite.events.EventType.EVT_SWAP_SPACE_DATA_EVICTED;

/**
 * Test for cache swap.
 */
public class GridCacheSwapSelfTest extends GridCommonAbstractTest {
    /** Entry count. */
    private static final int ENTRY_CNT = 1000;

    /** Swap count. */
    private final AtomicInteger swapCnt = new AtomicInteger();

    /** Unswap count. */
    private final AtomicInteger unswapCnt = new AtomicInteger();

    /** Saved versions. */
    private final Map<Integer, Object> versions = new HashMap<>();

    /** */
    private final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private boolean swapEnabled = true;

    /** PeerClassLoading excluded. */
    private boolean excluded;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(disco);

        cfg.setNetworkTimeout(2000);

        CacheConfiguration<?,?> cacheCfg = defaultCacheConfiguration();

        cacheCfg.setWriteSynchronizationMode(FULL_SYNC);
        cacheCfg.setCacheMode(REPLICATED);
        cacheCfg.setSwapEnabled(swapEnabled);
        cacheCfg.setIndexedTypes(
            Integer.class, CacheValue.class
        );

        cfg.setCacheConfiguration(cacheCfg);

        cfg.setDeploymentMode(SHARED);

        if (excluded)
            cfg.setPeerClassLoadingLocalClassPathExclude(GridCacheSwapSelfTest.class.getName(),
                CacheValue.class.getName());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        versions.clear();

        super.afterTestsStopped();
    }

    /**
     * @throws Exception If failed.
     */
    public void testDisabledSwap() throws Exception {
        swapEnabled = false;

        try {
            Ignite ignite = startGrids(1);

            SwapSpaceSpi swapSpi = ignite.configuration().getSwapSpaceSpi();

            assertNotNull(swapSpi);

            assertTrue(swapSpi instanceof NoopSwapSpaceSpi);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testEnabledSwap() throws Exception {
        try {
            Ignite ignite = startGrids(1);

            SwapSpaceSpi swapSpi = ignite.configuration().getSwapSpaceSpi();

            assertNotNull(swapSpi);

            assertFalse(swapSpi instanceof NoopSwapSpaceSpi);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("BusyWait")
    public void testSwapDeployment() throws Exception {
        try {
            Ignite ignite1 = startGrid(1);

            excluded = true;

            Ignite ignite2 = startGrid(2);

            IgniteCache<Integer, Object> cache1 = ignite1.cache(null);
            IgniteCache<Integer, Object> cache2 = ignite2.cache(null);

            Object v1 = new CacheValue(1);

            cache1.put(1, v1);

            info("Stored value in cache1 [v=" + v1 + ", ldr=" + v1.getClass().getClassLoader() + ']');

            Object v2 = cache2.get(1);

            assert v2 != null;

            info("Read value from cache2 [v=" + v2 + ", ldr=" + v2.getClass().getClassLoader() + ']');

            assert v2 != null;
            assert !v2.getClass().getClassLoader().equals(getClass().getClassLoader());
            assert v2.getClass().getClassLoader().getClass().getName().contains("GridDeploymentClassLoader");

            SwapListener lsnr = new SwapListener();

            ignite2.events().localListen(lsnr, EVT_CACHE_OBJECT_SWAPPED, EVT_CACHE_OBJECT_UNSWAPPED);

            evictAllEntries(cache2);

            assert lsnr.awaitSwap();

            assert cache2.get(1) != null;

            assert lsnr.awaitUnswap();

            ignite2.events().stopLocalListen(lsnr);

            lsnr = new SwapListener();

            ignite2.events().localListen(lsnr, EVT_CACHE_OBJECT_SWAPPED, EVT_CACHE_OBJECT_UNSWAPPED);

            evictAllEntries(cache2);

            assert lsnr.awaitSwap();

            stopGrid(1);

            boolean success = false;

            for (int i = 0; i < 6; i++) {
                success = cache2.get(1) == null;

                if (success)
                    break;
                else if (i < 2) {
                    info("Sleeping to wait for cache clear.");

                    Thread.sleep(500);
                }
            }

            assert success;
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @param cache Cache.
     */
    private void evictAllEntries(IgniteCache<Integer, Object> cache) {
        Set<Integer> keys = new HashSet<>();

        for (Cache.Entry<Integer, Object> e : cache.localEntries())
            keys.add(e.getKey());

        cache.localEvict(keys);
    }

    /**
     * @throws Exception If failed.
     */
    public void testSwapEviction() throws Exception {
        try {
            fail("https://issues.apache.org/jira/browse/IGNITE-599");

            final CountDownLatch evicted = new CountDownLatch(10);

            startGrids(1);

            grid(0).events().localListen(new IgnitePredicate<Event>() {
                @Override public boolean apply(Event evt) {
                    info("Received event: " + evt);

                    switch (evt.type()) {
                        case EVT_SWAP_SPACE_DATA_EVICTED:
                            assert evicted.getCount() > 0;

                            evicted.countDown();

                            break;
                    }

                    return true;
                }
            }, EVT_CACHE_OBJECT_SWAPPED, EVT_CACHE_OBJECT_UNSWAPPED, EVT_SWAP_SPACE_DATA_EVICTED);

            IgniteCache<Integer, CacheValue> cache = grid(0).cache(null);

            for (int i = 0; i< 20; i++) {
                cache.put(i, new CacheValue(i));

                cache.localEvict(Collections.singleton(i));
            }

            assert evicted.await(4, SECONDS) : "Entries were not evicted from swap: " + evicted.getCount();

            Collection<Cache.Entry<Integer, CacheValue>> res = cache.query(
                new SqlQuery(CacheValue.class, "val >= ? and val < ?").setArgs(0, 20)).getAll();

            int size = res.size();

            assert size == 10 : size;

            for (Cache.Entry<Integer, CacheValue> entry : res) {
                info("Entry: " + entry);

                assert entry != null;
                assert entry.getKey() != null;
                assert entry.getValue() != null;
                assert entry.getKey() == entry.getValue().value();
                assert entry.getKey() >= 10;
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testSwap() throws Exception {
        try {
            startGrids(1);

            grid(0).events().localListen(new IgnitePredicate<Event>() {
                @Override public boolean apply(Event evt) {
                    assert evt != null;

                    switch (evt.type()) {
                        case EVT_CACHE_OBJECT_SWAPPED:
                            swapCnt.incrementAndGet();

                            break;
                        case EVT_CACHE_OBJECT_UNSWAPPED:
                            unswapCnt.incrementAndGet();

                            break;
                    }

                    return true;
                }
            }, EVT_CACHE_OBJECT_SWAPPED, EVT_CACHE_OBJECT_UNSWAPPED);

            IgniteCache<Integer, CacheValue> cache = grid(0).cache(null);

            populate(grid(0));
            evictAll(cache);

            query(cache, 0, 200);        // Query swapped entries.
            unswap(cache, 200, 400);     // Check 'promote' method.
            unswapAll(cache, 400, 600);  // Check 'promoteAll' method.
            get(cache, 600, 800);        // Check 'get' method.
            peek(cache, 800, ENTRY_CNT); // Check 'peek' method in 'SWAP' mode.

            // Check that all entries were unswapped.
            for (int i = 0; i < ENTRY_CNT; i++) {
                CacheValue val = cache.localPeek(i);

                assert val != null;
                assert val.value() == i;
            }

            // Query unswapped entries.
            Collection<Cache.Entry<Integer, CacheValue>> res =
                cache.query(new SqlQuery(CacheValue.class, "val >= ? and val < ?").setArgs(0, ENTRY_CNT)).getAll();

            assert res.size() == ENTRY_CNT;

            for (Cache.Entry<Integer, CacheValue> entry : res) {
                assert entry != null;
                assert entry.getKey() != null;
                assert entry.getValue() != null;
                assert entry.getKey() == entry.getValue().value();
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testSwapIterator() throws Exception {
        try {
            startGrids(1);

            grid(0);

            IgniteCache<Integer, Integer> cache = grid(0).cache(null);

            for (int i = 0; i < 100; i++) {
                info("Putting: " + i);

                cache.put(i, i);

                cache.localEvict(Collections.singleton(i));
            }

            int i = 0;

            for (Cache.Entry<Integer, Integer> e : cache.localEntries(CachePeekMode.SWAP)) {

                Integer key = e.getKey();

                info("Key: " + key);

                i++;

                cache.remove(e.getKey());

                assertNull(cache.get(key));
            }

            assertEquals(100, i);

            assert cache.size() == 0;
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * Populates cache.
     *
     * @param ignite Ignite.
     * @throws Exception In case of error.
     */
    private void populate(Ignite ignite) throws Exception {
        resetCounters();

        for (int i = 0; i < ENTRY_CNT; i++) {
            ignite.<Integer, CacheValue>cache(null).put(i, new CacheValue(i));

            CacheValue val = ignite.<Integer, CacheValue>cache(null).localPeek(i);

            assert val != null;
            assert val.value() == i;

            GridCacheEntryEx entry = ((IgniteKernal)ignite).internalCache().peekEx(i);

            assert entry != null;

            versions.put(i, entry.version());
        }

        assert swapCnt.get() == 0;
        assert unswapCnt.get() == 0;
    }

    /**
     * Evicts all entries in cache.
     *
     * @param cache Cache.
     * @throws Exception In case of error.
     */
    private void evictAll(IgniteCache<Integer, CacheValue> cache) throws Exception {
        resetCounters();

        Set<Integer> keys = new HashSet<>();

        for (Cache.Entry<Integer, CacheValue> e : cache.localEntries())
            keys.add(e.getKey());

        cache.localEvict(keys);

        for (int i = 0; i < ENTRY_CNT; i++)
            assert cache.localPeek(i, CachePeekMode.ONHEAP) == null;

        assert swapCnt.get() == ENTRY_CNT;
        assert unswapCnt.get() == 0;
    }

    /**
     * Runs SQL query and checks result.
     *
     * @param cache Cache.
     * @param lowerBound Lower key bound.
     * @param upperBound Upper key bound.
     * @throws Exception In case of error.
     */
    private void query(IgniteCache<Integer, CacheValue> cache, int lowerBound, int upperBound) throws Exception {
        resetCounters();

        Collection<Cache.Entry<Integer, CacheValue>> res =
            cache.query(new SqlQuery(CacheValue.class, "val >= ? and val < ?").
                setArgs(lowerBound, upperBound)).getAll();

        assert res.size() == upperBound - lowerBound;

        for (Cache.Entry<Integer, CacheValue> entry : res) {
            assert entry != null;
            assert entry.getKey() != null;
            assert entry.getValue() != null;
            assert entry.getKey() == entry.getValue().value();
        }

        assert swapCnt.get() == 0;
        assert unswapCnt.get() == 0;

        checkEntries(cache, lowerBound, upperBound);

        assert swapCnt.get() == 0;
        assert unswapCnt.get() == upperBound - lowerBound;
    }

    /**
     * Unswaps entries and checks result.
     *
     * @param cache Cache.
     * @param lowerBound Lower key bound.
     * @param upperBound Upper key bound.
     * @throws Exception In case of error.
     */
    private void unswap(IgniteCache<Integer, CacheValue> cache, int lowerBound, int upperBound) throws Exception {
        resetCounters();

        assertEquals(0, swapCnt.get());
        assertEquals(0, unswapCnt.get());

        for (int i = lowerBound; i < upperBound; i++) {
            assert cache.localPeek(i, CachePeekMode.ONHEAP) == null;

            cache.localPromote(Collections.singleton(i));

            CacheValue val = cache.localPeek(i);

            assertNotNull(val);
            assertEquals(i, val.value());

            assertEquals(i - lowerBound + 1, unswapCnt.get());
        }

        assertEquals(0, swapCnt.get());
        assertEquals(unswapCnt.get(), upperBound - lowerBound);

        checkEntries(cache, lowerBound, upperBound);

        assertEquals(0, swapCnt.get());
        assertEquals(unswapCnt.get(), upperBound - lowerBound);
    }

    /**
     * Unswaps entries and checks result.
     *
     * @param cache Cache.
     * @param lowerBound Lower key bound.
     * @param upperBound Upper key bound.
     * @throws Exception In case of error.
     */
    private void unswapAll(IgniteCache<Integer, CacheValue> cache, int lowerBound, int upperBound) throws Exception {
        resetCounters();

        Set<Integer> keys = new HashSet<>();

        for (int i = lowerBound; i < upperBound; i++) {
            assert cache.localPeek(i, CachePeekMode.ONHEAP) == null;

            keys.add(i);
        }

        cache.localPromote(keys);

        assert swapCnt.get() == 0;
        assert unswapCnt.get() == upperBound - lowerBound;

        checkEntries(cache, lowerBound, upperBound);

        assert swapCnt.get() == 0;
        assert unswapCnt.get() == upperBound - lowerBound;
    }

    /**
     * Unswaps entries via {@code get} method and checks result.
     *
     * @param cache Cache.
     * @param lowerBound Lower key bound.
     * @param upperBound Upper key bound.
     * @throws Exception In case of error.
     */
    private void get(IgniteCache<Integer, CacheValue> cache, int lowerBound, int upperBound) throws Exception {
        resetCounters();

        for (int i = lowerBound; i < upperBound; i++) {
            assert cache.localPeek(i, CachePeekMode.ONHEAP) == null;

            CacheValue val = cache.get(i);

            assert val != null;
            assert val.value() == i;
        }

        assert swapCnt.get() == 0;
        assert unswapCnt.get() == upperBound - lowerBound;

        checkEntries(cache, lowerBound, upperBound);

        assert swapCnt.get() == 0;
        assert unswapCnt.get() == upperBound - lowerBound;
    }

    /**
     * Peeks entries in {@code SWAP} mode and checks result.
     *
     * @param cache Cache.
     * @param lowerBound Lower key bound.
     * @param upperBound Upper key bound.
     * @throws Exception In case of error.
     */
    private void peek(IgniteCache<Integer, CacheValue> cache, int lowerBound, int upperBound) throws Exception {
        resetCounters();

        for (int i = lowerBound; i < upperBound; i++) {
            assert cache.localPeek(i, CachePeekMode.ONHEAP) == null;

            CacheValue val = cache.localPeek(i, CachePeekMode.SWAP);

            assert val != null;
            assert val.value() == i;
        }

        assert swapCnt.get() == 0;
        assert unswapCnt.get() == 0;

        checkEntries(cache, lowerBound, upperBound);

        assert swapCnt.get() == 0;
        assert unswapCnt.get() == upperBound - lowerBound;
    }

    /**
     * Resets event counters.
     */
    private void resetCounters() {
        swapCnt.set(0);
        unswapCnt.set(0);
    }

    /**
     * Checks that entries in cache are correct after being unswapped.
     * If entry is still swapped, it will be unswapped in this method.
     *
     * @param cache Cache.
     * @param lowerBound Lower key bound.
     * @param upperBound Upper key bound.
     * @throws Exception In case of error.
     */
    private void checkEntries(IgniteCache<Integer, CacheValue> cache,
        int lowerBound, int upperBound) throws Exception {
        for (int i = lowerBound; i < upperBound; i++) {
            cache.localPromote(Collections.singleton(i));

            GridCacheEntryEx entry = dht(cache).entryEx(i);

            assert entry != null;
            assert entry.key() != null;

            CacheValue val = CU.value(entry.rawGet(), entry.context(), false);

            assert val != null;
            assertEquals(CU.value(entry.key(), entry.context(), false), new Integer(val.value()));
            assert entry.version().equals(versions.get(i));
        }
    }

    /**
     *
     */
    private static class CacheValue {
        /** Value. */
        @QuerySqlField
        private final int val;

        /**
         * @param val Value.
         */
        private CacheValue(int val) {
            this.val = val;
        }

        /**
         * @return Value.
         */
        public int value() {
            return val;
        }
    }

    /**
     *
     */
    private class SwapListener implements IgnitePredicate<Event> {
        /** */
        private final CountDownLatch swapLatch = new CountDownLatch(1);

        /** */
        private final CountDownLatch unswapLatch = new CountDownLatch(1);

        /** {@inheritDoc} */
        @Override public boolean apply(Event evt) {
            assert evt != null;

            info("Received event: " + evt);

            switch (evt.type()) {
                case EVT_CACHE_OBJECT_SWAPPED:
                    swapLatch.countDown();

                    break;
                case EVT_CACHE_OBJECT_UNSWAPPED:
                    unswapLatch.countDown();

                    break;
            }

            return true;
        }

        /**
         * @return {@code True} if await succeeded.
         * @throws InterruptedException If interrupted.
         */
        boolean awaitSwap() throws InterruptedException {
            return swapLatch.await(5000, MILLISECONDS);
        }

        /**
         * @return {@code True} if await succeeded.
         * @throws InterruptedException If interrupted.
         */
        boolean awaitUnswap() throws InterruptedException {
            return unswapLatch.await(5000, MILLISECONDS);
        }
    }
}