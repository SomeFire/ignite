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

package org.apache.ignite.internal.visor.query;

import java.io.Serializable;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryType;

/**
 * Descriptor of running query.
 */
public class VisorRunningQuery implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private long id;

    /** Query text. */
    private String qry;

    /** Query type. */
    private GridCacheQueryType qryType;

    /** Cache name for query. */
    private String cache;

    /** */
    private long startTime;

    /** */
    private boolean cancellable;

    /** */
    private boolean loc;

    /**
     * @param id Query ID.
     * @param qry Query text.
     * @param qryType Query type.
     * @param cache Cache where query was executed.
     * @param startTime Query start time.
     * @param cancellable {@code true} if query can be canceled.
     * @param loc {@code true} if query is local.
     */
    public VisorRunningQuery(long id, String qry, GridCacheQueryType qryType, String cache, long startTime,
        boolean cancellable, boolean loc) {
        this.id = id;
        this.qry = qry;
        this.qryType = qryType;
        this.cache = cache;
        this.startTime = startTime;
        this.cancellable = cancellable;
        this.loc = loc;
    }

    /**
     * @return Query ID.
     */
    public long id() {
        return id;
    }

    /**
     * @return Query txt.
     */
    public String query() {
        return qry;
    }

    /**
     * @return Query type.
     */
    public GridCacheQueryType queryType() {
        return qryType;
    }

    /**
     * @return Cache name.
     */
    public String getCache() {
        return cache;
    }

    /**
     * @return Query start time.
     */
    public long getStartTime() {
        return startTime;
    }

    /**
     * @return {@code true} if query can be cancelled.
     */
    public boolean isCancelable() {
        return cancellable;
    }

    /**
     * @return {@code true} if query is local.
     */
    public boolean isLocal() {
        return loc;
    }
}
