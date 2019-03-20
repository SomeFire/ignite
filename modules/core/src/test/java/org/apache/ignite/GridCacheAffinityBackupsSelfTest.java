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

package org.apache.ignite;

import java.util.Collection;
import java.util.HashSet;
import java.util.UUID;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Test;

/**
 * Tests affinity function with different number of backups.
 */
public class GridCacheAffinityBackupsSelfTest extends GridCommonAbstractTest {
    /** Number of backups. */
    private int backups;

    /** */
    private int nodesCnt = 5;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setCacheMode(CacheMode.PARTITIONED);
        ccfg.setBackups(backups);
        ccfg.setAffinity(new RendezvousAffinityFunction());

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /**
     *
     */
    @After
    public static void clearHandler() {
        expectNothing();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRendezvousBackups() throws Exception {
        expectFailure(NodeStoppingException.class);
        expectFailure(IgniteCheckedException.class, "Node is stopping: ");
        expectFailure(InterruptedException.class);

        for (int i = 0; i < nodesCnt; i++)
            checkBackups(i);
    }

    /**
     * @param backups Number of backups.
     * @throws Exception If failed.
     */
    private void checkBackups(int backups) throws Exception {
        this.backups = backups;

        startGridsMultiThreaded(nodesCnt, true);

        try {
            IgniteCache<Object, Object> cache = jcache(0);

            Collection<UUID> members = new HashSet<>();

            for (int i = 0; i < 10000; i++) {
                Collection<ClusterNode> nodes = affinity(cache).mapKeyToPrimaryAndBackups(i);

                assertEquals(backups + 1, nodes.size());

                for (ClusterNode n : nodes)
                    members.add(n.id());
            }

            assertEquals(nodesCnt, members.size());
        }
        finally {
            stopAllGrids();
        }
    }
}
