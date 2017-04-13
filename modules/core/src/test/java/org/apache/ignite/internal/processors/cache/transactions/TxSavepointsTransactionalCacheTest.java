package org.apache.ignite.internal.processors.cache.transactions;

import org.apache.ignite.IgniteCache;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.LOCAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;

/**
 *
 */
public class TxSavepointsTransactionalCacheTest extends TxSavepointsTest {

    /** {@inheritDoc} */
    @Override
    protected void checkResult(String errMsg, IgniteCache<Integer, Integer> cache) {
        int i = 0;
        for (i = 1; i <= 12; i++) {
            assertEquals(errMsg, (Integer) i, cache.get(i));
        }
        for (i = 13; i <= 16; i++) {
            assertEquals(errMsg, null, cache.get(i));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransLocal() throws Exception {
        checkSavepoints(cacheConfig(TRANSACTIONAL, LOCAL));
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransReplicated() throws Exception {
        checkSavepoints(cacheConfig(TRANSACTIONAL, REPLICATED));
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransPartitioned() throws Exception {
        checkSavepoints(cacheConfig(TRANSACTIONAL, PARTITIONED));
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransLocalMultipleCaches() throws Exception {
        checkSavepointsWithTwoCaches(cacheConfig(TRANSACTIONAL, LOCAL));
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransReplicatedMultipleCaches() throws Exception {
        checkSavepointsWithTwoCaches(cacheConfig(TRANSACTIONAL, REPLICATED));
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransPartitionedMultipleCaches() throws Exception {
        checkSavepointsWithTwoCaches(cacheConfig(TRANSACTIONAL, PARTITIONED));
    }
}