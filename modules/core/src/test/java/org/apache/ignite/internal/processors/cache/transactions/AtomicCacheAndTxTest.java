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
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheMode.REPLICATED;

public class AtomicCacheAndTxTest extends GridCommonAbstractTest {

	/** {@inheritDoc} */
	@Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
		IgniteConfiguration cfg = super.getConfiguration(gridName);

		/*TcpDiscoverySpi disc = new TcpDiscoverySpi();

		disc.setIpFinder(new TcpDiscoveryVmIpFinder(true));

		cfg.setDiscoverySpi(disc);*/

		CacheConfiguration ccfg  = new CacheConfiguration();

		ccfg.setAtomicityMode(ATOMIC);

/*
		ccfg.setCacheMode(REPLICATED);

		ccfg.setBackups(1);

		ccfg.setSwapEnabled(true);
*/

		cfg.setCacheConfiguration(ccfg);

		return cfg;
	}

	@Override
	protected void beforeTestsStarted() throws Exception {
		super.beforeTestsStarted();
		startGrid(0);
	}

	@Override
	protected void afterTest() throws Exception {
		grid(0).cache(null).remove(1);
	}

	public void testAllowed() throws Exception {
		testTransaction(grid(0).cache(null).withAllowInTx(), true);
	}

	public void testNotAllowed() {
		testTransaction(grid(0).cache(null), false);
	}

	private void testTransaction(IgniteCache<Integer, Integer> cache, boolean allow) {
		IgniteException err = null;
		try (Transaction tx = grid(0).transactions().txStart()) {
			cache.put(1, 1);
			tx.commit();
		} catch (IgniteException e) {
			err = e;
		}
		assertTrue(allow && err == null ||
				!allow && err != null && err.getMessage().startsWith("Transaction spans operations on atomic cache"));

	}
}