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

package org.apache.ignite.internal.processors.cache.persistence.db;

import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.OpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteIllegalStateException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.GridCacheUtils;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemandMessage;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIODecorator;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTestWithNoOpHandler;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;

/**
 * A set of tests that check correctness of logical recovery performed during node start.
 */
public class IgniteLogicalRecoveryTest extends GridCommonAbstractTestWithNoOpHandler {
    /** */
    private static final int[] EVTS_DISABLED = {};

    /** Shared group name. */
    private static final String SHARED_GROUP_NAME = "group";

    /** Dynamic cache prefix. */
    private static final String DYNAMIC_CACHE_PREFIX = "dynamic-cache-";

    /** Cache prefix. */
    private static final String CACHE_PREFIX = "cache-";

    /** Io factory. */
    private FileIOFactory ioFactory;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setIncludeEventTypes(EVTS_DISABLED);

        cfg.setConsistentId(igniteInstanceName);

        cfg.setCacheConfiguration(
            cacheConfiguration(CACHE_PREFIX + 0, CacheMode.PARTITIONED, CacheAtomicityMode.ATOMIC),
            cacheConfiguration(CACHE_PREFIX + 1, CacheMode.PARTITIONED, CacheAtomicityMode.TRANSACTIONAL),
            cacheConfiguration(CACHE_PREFIX + 2, CacheMode.REPLICATED, CacheAtomicityMode.ATOMIC),
            cacheConfiguration(CACHE_PREFIX + 3, CacheMode.REPLICATED, CacheAtomicityMode.TRANSACTIONAL),
            cacheConfiguration(CACHE_PREFIX + 4, SHARED_GROUP_NAME, CacheMode.PARTITIONED, CacheAtomicityMode.TRANSACTIONAL),
            cacheConfiguration(CACHE_PREFIX + 5, SHARED_GROUP_NAME, CacheMode.PARTITIONED, CacheAtomicityMode.TRANSACTIONAL)
        );

        DataStorageConfiguration dsCfg = new DataStorageConfiguration()
            .setWalMode(WALMode.LOG_ONLY)
            .setCheckpointFrequency(1024 * 1024 * 1024) // Disable automatic checkpoints.
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setName("dflt")
                    .setInitialSize(256 * 1024 * 1024)
                    .setMaxSize(256 * 1024 * 1024)
                    .setPersistenceEnabled(true)
            );

        cfg.setDataStorageConfiguration(dsCfg);

        if (ioFactory != null)
            dsCfg.setFileIOFactory(ioFactory);

        TestRecordingCommunicationSpi spi = new TestRecordingCommunicationSpi();

        spi.record(GridDhtPartitionDemandMessage.class);

        cfg.setCommunicationSpi(spi);

        return cfg;
    }

    /**
     * @param name Name.
     * @param cacheMode Cache mode.
     * @param atomicityMode Atomicity mode.
     */
    private CacheConfiguration<Object, Object> cacheConfiguration(String name, CacheMode cacheMode, CacheAtomicityMode atomicityMode) {
        return cacheConfiguration(name, null, cacheMode, atomicityMode);
    }

    /**
     * @param name Name.
     * @param groupName Group name.
     * @param cacheMode Cache mode.
     * @param atomicityMode Atomicity mode.
     */
    protected CacheConfiguration<Object, Object> cacheConfiguration(String name, @Nullable String groupName, CacheMode cacheMode, CacheAtomicityMode atomicityMode) {
        CacheConfiguration<Object, Object> cfg = new CacheConfiguration<>(name)
            .setGroupName(groupName)
            .setCacheMode(cacheMode)
            .setAtomicityMode(atomicityMode)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
            .setBackups(2)
            .setAffinity(new RendezvousAffinityFunction(false, 32));

        cfg.setIndexedTypes(Integer.class, Integer.class);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        System.setProperty(GridCacheDatabaseSharedManager.IGNITE_PDS_SKIP_CHECKPOINT_ON_NODE_STOP, "true");
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        System.clearProperty(GridCacheDatabaseSharedManager.IGNITE_PDS_SKIP_CHECKPOINT_ON_NODE_STOP);
    }

    /**
     *
     */
    public void testRecoveryOnJoinToActiveCluster() throws Exception {
        IgniteEx crd = (IgniteEx) startGridsMultiThreaded(3);

        crd.cluster().active(true);

        IgniteEx node = grid(2);

        AggregateCacheLoader cacheLoader = new AggregateCacheLoader(node);

        cacheLoader.loadByTime(5_000).get();

        forceCheckpoint();

        cacheLoader.loadByTime(5_000).get();

        stopGrid(2, true);

        node = startGrid(2);

        awaitPartitionMapExchange();

        cacheLoader.consistencyCheck(node);

        checkNoRebalanceAfterRecovery();
    }

    /**
     *
     */
    public void testRecoveryOnJoinToInactiveCluster() throws Exception {
        IgniteEx crd = (IgniteEx) startGridsMultiThreaded(3);

        crd.cluster().active(true);

        IgniteEx node = grid(2);

        AggregateCacheLoader cacheLoader = new AggregateCacheLoader(node);

        cacheLoader.loadByTime(5_000).get();

        forceCheckpoint();

        cacheLoader.loadByTime(5_000).get();

        stopGrid(2, true);

        crd.cluster().active(false);

        node = startGrid(2);

        crd.cluster().active(true);

        awaitPartitionMapExchange();

        checkNoRebalanceAfterRecovery();

        cacheLoader.consistencyCheck(node);
    }

    /**
     *
     */
    public void testRecoveryOnDynamicallyStartedCaches() throws Exception {
        List<CacheConfiguration> dynamicCaches = Lists.newArrayList(
            cacheConfiguration(DYNAMIC_CACHE_PREFIX + 0, CacheMode.PARTITIONED, CacheAtomicityMode.TRANSACTIONAL),
            cacheConfiguration(DYNAMIC_CACHE_PREFIX + 1, CacheMode.REPLICATED, CacheAtomicityMode.TRANSACTIONAL),
            cacheConfiguration(DYNAMIC_CACHE_PREFIX + 2, CacheMode.PARTITIONED, CacheAtomicityMode.ATOMIC),
            cacheConfiguration(DYNAMIC_CACHE_PREFIX + 3, CacheMode.REPLICATED, CacheAtomicityMode.ATOMIC)
        );

        doTestWithDynamicCaches(dynamicCaches);
    }

    /**
     *
     */
    public void testRecoveryWithMvccCaches() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-10052");

        List<CacheConfiguration> dynamicCaches = Lists.newArrayList(
            cacheConfiguration(DYNAMIC_CACHE_PREFIX + 0, CacheMode.PARTITIONED, CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT),
            cacheConfiguration(DYNAMIC_CACHE_PREFIX + 1, CacheMode.REPLICATED, CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT)
        );

        doTestWithDynamicCaches(dynamicCaches);
    }

    /**
     * @param dynamicCaches Dynamic caches.
     */
    private void doTestWithDynamicCaches(List<CacheConfiguration> dynamicCaches) throws Exception {
        IgniteEx crd = (IgniteEx) startGridsMultiThreaded(3);

        crd.cluster().active(true);

        IgniteEx node = grid(2);

        node.getOrCreateCaches(dynamicCaches);

        AggregateCacheLoader cacheLoader = new AggregateCacheLoader(node);

        cacheLoader.loadByTime(5_000).get();

        forceCheckpoint();

        cacheLoader.loadByTime(5_000).get();

        stopGrid(2, true);

        startGrid(2);

        awaitPartitionMapExchange();

        checkNoRebalanceAfterRecovery();

        for (int idx = 0; idx < 3; idx++)
            cacheLoader.consistencyCheck(grid(idx));
    }

    /**
     *
     */
    public void testRecoveryOnJoinToDifferentBlt() throws Exception {
        IgniteEx crd = (IgniteEx) startGridsMultiThreaded(3);

        crd.cluster().active(true);

        IgniteEx node = grid(2);

        AggregateCacheLoader cacheLoader = new AggregateCacheLoader(node);

        cacheLoader.loadByTime(5_000).get();

        forceCheckpoint();

        cacheLoader.loadByTime(5_000).get();

        stopGrid(2, true);

        resetBaselineTopology();

        startGrid(2);

        resetBaselineTopology();

        awaitPartitionMapExchange();

        for (int idx = 0; idx < 3; idx++)
            cacheLoader.consistencyCheck(grid(idx));
    }

    /**
     *
     */
    public void testRecoveryOnCrushDuringCheckpointOnNodeStart() throws Exception {
        IgniteEx crd = (IgniteEx) startGridsMultiThreaded(3, false);

        crd.cluster().active(true);

        IgniteEx node = grid(2);

        AggregateCacheLoader cacheLoader = new AggregateCacheLoader(node);

        cacheLoader.loadByTime(5_000).get();

        forceCheckpoint();

        cacheLoader.loadByTime(5_000).get();

        stopGrid(2, false);

        ioFactory = new CheckpointFailIoFactory();

        IgniteInternalFuture startNodeFut = GridTestUtils.runAsync(() -> startGrid(2));

        try {
            startNodeFut.get();
        }
        catch (Exception expected) { }

        // Wait until node will leave cluster.
        GridTestUtils.waitForCondition(() -> {
            try {
                grid(2);
            }
            catch (IgniteIllegalStateException e) {
                return true;
            }

            return false;
        }, getTestTimeout());

        ioFactory = null;

        // Start node again and check recovery.
        startGrid(2);

        awaitPartitionMapExchange();

        checkNoRebalanceAfterRecovery();

        for (int idx = 0; idx < 3; idx++)
            cacheLoader.consistencyCheck(grid(idx));
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 120 * 1000;
    }

    /**
     * Method checks that there were no rebalance for all caches (excluding sys cache).
     */
    private void checkNoRebalanceAfterRecovery() {
        int sysCacheGroupId = CU.cacheId(GridCacheUtils.UTILITY_CACHE_NAME);

        List<Ignite> nodes = G.allGrids();

        for (Ignite node : nodes) {
            TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(node);

            List<Integer> rebalancedGroups = spi.recordedMessages(true).stream()
                .map(msg -> (GridDhtPartitionDemandMessage) msg)
                .map(msg -> msg.groupId())
                .filter(grpId -> grpId != sysCacheGroupId)
                .distinct()
                .collect(Collectors.toList());

            Assert.assertTrue("There was unexpected rebalance for some groups" +
                    " [node=" + node.name() + ", groups=" + rebalancedGroups + ']', rebalancedGroups.isEmpty());
        }
    }

    /**
     *
     */
    private static class AggregateCacheLoader {
        /** Ignite. */
        final IgniteEx ignite;

        /** Cache loaders. */
        final List<CacheLoader> cacheLoaders;

        /**
         * @param ignite Ignite.
         */
        public AggregateCacheLoader(IgniteEx ignite) {
            this.ignite = ignite;

            List<CacheLoader> cacheLoaders = new ArrayList<>();

            for (String cacheName : ignite.cacheNames())
                cacheLoaders.add(new CacheLoader(ignite, cacheName));

            this.cacheLoaders = cacheLoaders;
        }

        /**
         * @param timeMillis Loading time in milliseconds.
         */
        public IgniteInternalFuture<?> loadByTime(int timeMillis) {
            GridCompoundFuture<?, ?> loadFut = new GridCompoundFuture();

            for (CacheLoader cacheLoader : cacheLoaders) {
                long endTime = U.currentTimeMillis() + timeMillis;

                cacheLoader.stopPredicate = it -> U.currentTimeMillis() >= endTime;

                loadFut.add(GridTestUtils.runAsync(cacheLoader));
            }

            loadFut.markInitialized();

            return loadFut;
        }

        /**
         * @param ignite Ignite node to check consistency from.
         */
        public void consistencyCheck(IgniteEx ignite) {
            for (CacheLoader cacheLoader : cacheLoaders)
                cacheLoader.consistencyCheck(ignite);
        }
    }

    /**
     *
     */
    static class CacheLoader implements Runnable {
        /** Keys space. */
        static final int KEYS_SPACE = 3096;

        /** Ignite. */
        final IgniteEx ignite;

        /** Stop predicate. */
        volatile Predicate<IgniteEx> stopPredicate;

        /** Cache name. */
        final String cacheName;

        /** Local cache. */
        final Map<Integer, TestValue> locCache = new ConcurrentHashMap<>();

        /**
         * @param ignite Ignite.
         * @param cacheName Cache name.
         */
        public CacheLoader(IgniteEx ignite, String cacheName) {
            this.ignite = ignite;
            this.cacheName = cacheName;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            final Predicate<IgniteEx> predicate = stopPredicate;

            while (!predicate.test(ignite)) {
                ThreadLocalRandom rnd = ThreadLocalRandom.current();

                int key = rnd.nextInt(KEYS_SPACE);

                boolean remove = rnd.nextInt(100) <= 20;

                try {
                    IgniteCache<Object, Object> cache = ignite.getOrCreateCache(cacheName);

                    if (remove) {
                        cache.remove(key);

                        locCache.remove(key);
                    }
                    else {
                        int[] payload = new int[KEYS_SPACE];
                        Arrays.fill(payload, key);

                        TestValue val = new TestValue(key, payload);

                        cache.put(key, val);

                        locCache.put(key, val);
                    }

                    // Throttle against GC.
                    U.sleep(1);
                }
                catch (Exception ignored) { }
            }
        }

        /**
         *
         */
        public void consistencyCheck(IgniteEx ignite) {
            IgniteCache<Integer, TestValue> cache = ignite.getOrCreateCache(cacheName);

            for (int key = 0; key < KEYS_SPACE; key++) {
                TestValue expectedVal = locCache.get(key);
                TestValue actualVal = cache.get(key);

                Assert.assertEquals("Consistency check failed for: " + cache.getName() + ", key=" + key,
                    expectedVal, actualVal);
            }
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            CacheLoader loader = (CacheLoader) o;

            return Objects.equals(cacheName, loader.cacheName);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(cacheName);
        }
    }

    /**
     *
     */
    static class CheckpointFailIoFactory implements FileIOFactory {
        /** {@inheritDoc} */
        @Override public FileIO create(File file, OpenOption... modes) throws IOException {
            FileIO delegate = new RandomAccessFileIOFactory().create(file, modes);

            if (file.getName().contains("part-"))
                return new FileIODecorator(delegate) {
                    @Override public int write(ByteBuffer srcBuf) throws IOException {
                        throw new IOException("test");
                    }

                    @Override public int write(ByteBuffer srcBuf, long position) throws IOException {
                        throw new IOException("test");
                    }

                    @Override public int write(byte[] buf, int off, int len) throws IOException {
                        throw new IOException("test");
                    }
                };

            return delegate;
        }
    }

    /**
     * Test payload with indexed field.
     */
    static class TestValue {
        /** Indexed field. */
        @QuerySqlField(index = true)
        private final int indexedField;

        /** Payload. */
        private final int[] payload;

        /**
         * @param indexedField Indexed field.
         * @param payload Payload.
         */
        public TestValue(int indexedField, int[] payload) {
            this.indexedField = indexedField;
            this.payload = payload;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            TestValue testValue = (TestValue) o;

            return indexedField == testValue.indexedField &&
                Arrays.equals(payload, testValue.payload);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int result = Objects.hash(indexedField);

            result = 31 * result + Arrays.hashCode(payload);

            return result;
        }
    }
}
