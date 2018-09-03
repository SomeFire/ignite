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

import java.io.Externalizable;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.LinkedList;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.processor.EntryProcessor;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.IgniteCodeGeneratingFail;
import org.apache.ignite.internal.processors.cache.CacheEntryPredicate;
import org.apache.ignite.internal.processors.cache.CacheInvokeEntry;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheEntryRemovedException;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.IgniteExternalizableExpiryPolicy;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.lang.GridPeerDeployAware;
import org.apache.ignite.internal.util.tostring.GridToStringBuilder;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.apache.ignite.thread.IgniteThread;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.GridCacheOperation.READ;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.TRANSFORM;

/**
 * Transaction entry. Note that it is essential that this class does not override
 * {@link #equals(Object)} method, as transaction entries should use referential
 * equality.
 */
@IgniteCodeGeneratingFail // Field filters should not be generated by MessageCodeGenerator.
public class IgniteTxEntry implements GridPeerDeployAware, Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** Dummy version for non-existing entry read in SERIALIZABLE transaction. */
    public static final GridCacheVersion SER_READ_EMPTY_ENTRY_VER = new GridCacheVersion(0, 0, 0);

    /** Dummy version for any existing entry read in SERIALIZABLE transaction. */
    public static final GridCacheVersion SER_READ_NOT_EMPTY_VER = new GridCacheVersion(0, 0, 1);

    /** */
    public static final GridCacheVersion GET_ENTRY_INVALID_VER_UPDATED = new GridCacheVersion(0, 0, 2);

    /** */
    public static final GridCacheVersion GET_ENTRY_INVALID_VER_AFTER_GET = new GridCacheVersion(0, 0, 3);

    /** Skip store flag bit mask. */
    private static final int TX_ENTRY_SKIP_STORE_FLAG_MASK = 0x01;

    /** Keep binary flag. */
    private static final int TX_ENTRY_KEEP_BINARY_FLAG_MASK = 0x02;

    /** Flag indicating that old value for 'invoke' operation was non null on primary node. */
    private static final int TX_ENTRY_OLD_VAL_ON_PRIMARY = 0x04;

    /** Flag indicating that near cache is enabled on originating node and it should be added as reader. */
    private static final int TX_ENTRY_ADD_READER_FLAG_MASK = 0x08;

    /** Prepared flag updater. */
    private static final AtomicIntegerFieldUpdater<IgniteTxEntry> PREPARED_UPD =
        AtomicIntegerFieldUpdater.newUpdater(IgniteTxEntry.class, "prepared");

    /** Owning transaction. */
    @GridToStringExclude
    @GridDirectTransient
    private IgniteInternalTx tx;

    /** Cache key. */
    @GridToStringInclude
    private KeyCacheObject key;

    /** Cache ID. */
    private int cacheId;

    /** Transient tx key. */
    @GridDirectTransient
    private IgniteTxKey txKey;

    /** Cache value. */
    @GridToStringInclude
    private TxEntryValueHolder val = new TxEntryValueHolder();

    /** Visible value for peek. */
    @GridToStringInclude
    @GridDirectTransient
    private TxEntryValueHolder prevVal = new TxEntryValueHolder();

    /** Old value before update. */
    @GridToStringInclude
    private TxEntryValueHolder oldVal = new TxEntryValueHolder();

    /** Transform. */
    @GridToStringInclude
    @GridDirectTransient
    private Collection<T2<EntryProcessor<Object, Object, Object>, Object[]>> entryProcessorsCol;

    /** Transient field for calculated entry processor value. */
    @GridDirectTransient
    private T2<GridCacheOperation, CacheObject> entryProcessorCalcVal;

    /** Transform closure bytes. */
    @GridToStringExclude
    private byte[] transformClosBytes;

    /** Time to live. */
    private long ttl;

    /** DR expire time (explicit) */
    private long conflictExpireTime = CU.EXPIRE_TIME_CALCULATE;

    /** Conflict version. */
    private GridCacheVersion conflictVer;

    /** Explicit lock version if there is one. */
    @GridToStringInclude
    private GridCacheVersion explicitVer;

    /** DHT version. */
    @GridDirectTransient
    private volatile GridCacheVersion dhtVer;

    /** Put filters. */
    @GridToStringInclude
    private CacheEntryPredicate[] filters;

    /** Flag indicating whether filters passed. Used for fast-commit transactions. */
    @GridDirectTransient
    private boolean filtersPassed;

    /** Flag indicating that filter is set and can not be replaced. */
    @GridDirectTransient
    private boolean filtersSet;

    /** Underlying cache entry. */
    @GridDirectTransient
    private volatile GridCacheEntryEx entry;

    /** Cache registry. */
    @GridDirectTransient
    private GridCacheContext<?, ?> ctx;

    /** Prepared flag to prevent multiple candidate add. */
    @SuppressWarnings("UnusedDeclaration")
    @GridDirectTransient
    private transient volatile int prepared;

    /** Lock flag for collocated cache. */
    @GridDirectTransient
    private transient boolean locked;

    /** Assigned node ID (required only for partitioned cache). */
    @GridDirectTransient
    private UUID nodeId;

    /** Flag if this node is a back up node. */
    @GridDirectTransient
    private boolean locMapped;

    /** Expiry policy. */
    @GridDirectTransient
    private ExpiryPolicy expiryPlc;

    /** Expiry policy transfer flag. */
    @GridDirectTransient
    private boolean transferExpiryPlc;

    /** Expiry policy bytes. */
    private byte[] expiryPlcBytes;

    /** Additional flags. */
    private byte flags;

    /** Partition update counter. */
    @GridDirectTransient
    private long partUpdateCntr;

    /** */
    private GridCacheVersion serReadVer;

    /**
     * Required by {@link Externalizable}
     */
    public IgniteTxEntry() {
        /* No-op. */
    }

    /**
     * This constructor is meant for remote transactions.
     *
     * @param ctx Cache registry.
     * @param tx Owning transaction.
     * @param op Operation.
     * @param val Value.
     * @param ttl Time to live.
     * @param conflictExpireTime DR expire time.
     * @param entry Cache entry.
     * @param conflictVer Data center replication version.
     * @param skipStore Skip store flag.
     */
    public IgniteTxEntry(GridCacheContext<?, ?> ctx,
        IgniteInternalTx tx,
        GridCacheOperation op,
        CacheObject val,
        long ttl,
        long conflictExpireTime,
        GridCacheEntryEx entry,
        @Nullable GridCacheVersion conflictVer,
        boolean skipStore,
        boolean keepBinary
    ) {
        assert ctx != null;
        assert tx != null;
        assert op != null;
        assert entry != null;

        this.ctx = ctx;
        this.tx = tx;
        this.val.value(op, val, false, false);
        this.entry = entry;
        this.ttl = ttl;
        this.conflictExpireTime = conflictExpireTime;
        this.conflictVer = conflictVer;

        skipStore(skipStore);
        keepBinary(keepBinary);

        key = entry.key();

        cacheId = entry.context().cacheId();
    }

    /**
     * This constructor is meant for local transactions.
     *
     * @param ctx Cache registry.
     * @param tx Owning transaction.
     * @param op Operation.
     * @param val Value.
     * @param entryProcessor Entry processor.
     * @param invokeArgs Optional arguments for EntryProcessor.
     * @param ttl Time to live.
     * @param entry Cache entry.
     * @param filters Put filters.
     * @param conflictVer Data center replication version.
     * @param skipStore Skip store flag.
     * @param addReader Add reader flag.
     */
    public IgniteTxEntry(GridCacheContext<?, ?> ctx,
        IgniteInternalTx tx,
        GridCacheOperation op,
        CacheObject val,
        EntryProcessor<Object, Object, Object> entryProcessor,
        Object[] invokeArgs,
        long ttl,
        GridCacheEntryEx entry,
        CacheEntryPredicate[] filters,
        GridCacheVersion conflictVer,
        boolean skipStore,
        boolean keepBinary,
        boolean addReader
    ) {
        assert ctx != null;
        assert tx != null;
        assert op != null;
        assert entry != null;

        this.ctx = ctx;
        this.tx = tx;
        this.val.value(op, val, false, false);
        this.entry = entry;
        this.ttl = ttl;
        this.filters = filters;
        this.conflictVer = conflictVer;

        skipStore(skipStore);
        keepBinary(keepBinary);
        addReader(addReader);

        if (entryProcessor != null)
            addEntryProcessor(entryProcessor, invokeArgs);

        key = entry.key();

        cacheId = entry.context().cacheId();
    }

    /**
     * @return Cache context for this tx entry.
     */
    public GridCacheContext<?, ?> context() {
        return ctx;
    }

    /**
     * @param ctx Cache context for this tx entry.
     */
    public void context(GridCacheContext<?, ?> ctx) {
        this.ctx = ctx;
    }

    /**
     * @return Flag indicating if this entry is affinity mapped to the same node.
     */
    public boolean locallyMapped() {
        return locMapped;
    }

    /**
     * @param locMapped Flag indicating if this entry is affinity mapped to the same node.
     */
    public void locallyMapped(boolean locMapped) {
        this.locMapped = locMapped;
    }

    /**
     * @param ctx Context.
     * @return Clean copy of this entry.
     */
    public IgniteTxEntry cleanCopy(GridCacheContext<?, ?> ctx) {
        IgniteTxEntry cp = new IgniteTxEntry();

        cp.key = key;
        cp.cacheId = cacheId;
        cp.ctx = ctx;

        cp.val = new TxEntryValueHolder();

        cp.filters = filters;
        cp.val.value(val.op(), val.value(), val.hasWriteValue(), val.hasReadValue());
        cp.entryProcessorsCol = entryProcessorsCol;
        cp.ttl = ttl;
        cp.conflictExpireTime = conflictExpireTime;
        cp.explicitVer = explicitVer;
        cp.conflictVer = conflictVer;
        cp.expiryPlc = expiryPlc;
        cp.flags = flags;
        cp.serReadVer = serReadVer;

        return cp;
    }

    /**
     * @return Copy of this entry.
     */
    public IgniteTxEntry copy() {
        IgniteTxEntry e = cleanCopy(ctx);

        e.cached(entry);

        if (locked)
            e.markLocked();

        return e;
    }

    /**
     * @return Node ID.
     */
    public UUID nodeId() {
        return nodeId;
    }

    /**
     * @param nodeId Node ID.
     */
    public void nodeId(UUID nodeId) {
        this.nodeId = nodeId;
    }

    /**
     * @return DHT version.
     */
    public GridCacheVersion dhtVersion() {
        return dhtVer;
    }

    /**
     * @param dhtVer DHT version.
     */
    public void dhtVersion(GridCacheVersion dhtVer) {
        this.dhtVer = dhtVer;
    }

    /**
     * @return {@code True} if tx entry was marked as locked.
     */
    public boolean locked() {
        return locked;
    }

    /**
     * Marks tx entry as locked.
     */
    public void markLocked() {
        locked = true;
    }

    /**
     * Sets partition counter.
     *
     * @param partCntr Partition counter.
     */
    public void updateCounter(long partCntr) {
        this.partUpdateCntr = partCntr;
    }

    /**
     * @return Partition index.
     */
    public long updateCounter() {
        return partUpdateCntr;
    }

    /**
     * @param val Value to set.
     */
    public void setAndMarkValid(CacheObject val) {
        setAndMarkValid(op(), val, this.val.hasWriteValue(), this.val.hasReadValue());
    }

    /**
     * @param op Operation.
     * @param val Value to set.
     */
    void setAndMarkValid(GridCacheOperation op, CacheObject val) {
        setAndMarkValid(op, val, this.val.hasWriteValue(), this.val.hasReadValue());
    }

    /**
     * @param op Operation.
     * @param val Value to set.
     * @param hasReadVal Has read value flag.
     * @param hasWriteVal Has write value flag.
     */
    void setAndMarkValid(GridCacheOperation op, CacheObject val, boolean hasWriteVal, boolean hasReadVal) {
        this.val.value(op, val, hasWriteVal, hasReadVal);

        markValid();
    }

    /**
     * Marks this entry as value-has-bean-read. Effectively, makes values enlisted to transaction visible
     * to further peek operations.
     */
    public void markValid() {
        prevVal.value(val.op(), val.value(), val.hasWriteValue(), val.hasReadValue());
    }

    /**
     * Marks entry as prepared.
     *
     * @return True if entry was marked prepared by this call.
     */
    boolean markPrepared() {
        return PREPARED_UPD.compareAndSet(this, 0, 1);
    }

    /**
     * @return Entry key.
     */
    public KeyCacheObject key() {
        return key;
    }

    /**
     * @return Cache ID.
     */
    public int cacheId() {
        return cacheId;
    }

    /**
     * Sets skip store flag value.
     *
     * @param skipStore Skip store flag.
     */
    public void skipStore(boolean skipStore) {
        setFlag(skipStore, TX_ENTRY_SKIP_STORE_FLAG_MASK);
    }

    /**
     * @return Skip store flag.
     */
    public boolean skipStore() {
        return isFlag(TX_ENTRY_SKIP_STORE_FLAG_MASK);
    }

    /**
     * @param oldValOnPrimary {@code True} If old value for was non null on primary node.
     */
    public void oldValueOnPrimary(boolean oldValOnPrimary) {
        setFlag(oldValOnPrimary, TX_ENTRY_OLD_VAL_ON_PRIMARY);
    }

    /**
     * @return {@code True} If old value for 'invoke' operation was non null on primary node.
     */
    boolean oldValueOnPrimary() {
        return isFlag(TX_ENTRY_OLD_VAL_ON_PRIMARY);
    }

    /**
     * Sets keep binary flag value.
     *
     * @param keepBinary Keep binary flag value.
     */
    public void keepBinary(boolean keepBinary) {
        setFlag(keepBinary, TX_ENTRY_KEEP_BINARY_FLAG_MASK);
    }

    /**
     * @return Keep binary flag value.
     */
    public boolean keepBinary() {
        return isFlag(TX_ENTRY_KEEP_BINARY_FLAG_MASK);
    }

    /**
     * @param addReader Add reader flag.
     */
    public void addReader(boolean addReader) {
        setFlag(addReader, TX_ENTRY_ADD_READER_FLAG_MASK);
    }

    /**
     * @return Add reader flag.
     */
    public boolean addReader() {
        return isFlag(TX_ENTRY_ADD_READER_FLAG_MASK);
    }

    /**
     * Sets flag mask.
     *
     * @param flag Set or clear.
     * @param mask Mask.
     */
    private void setFlag(boolean flag, int mask) {
        flags = flag ? (byte)(flags | mask) : (byte)(flags & ~mask);
    }

    /**
     * Reads flag mask.
     *
     * @param mask Mask to read.
     * @return Flag value.
     */
    private boolean isFlag(int mask) {
        return (flags & mask) != 0;
    }

    /**
     * @return Tx key.
     */
    public IgniteTxKey txKey() {
        if (txKey == null)
            txKey = new IgniteTxKey(key, cacheId);

        return txKey;
    }

    /**
     * @return Underlying cache entry.
     */
    public GridCacheEntryEx cached() {
        return entry;
    }

    /**
     * @param entry Cache entry.
     */
    public void cached(GridCacheEntryEx entry) {
        assert entry == null || entry.context() == ctx : "Invalid entry assigned to tx entry [txEntry=" + this +
            ", entry=" + entry +
            ", ctxNear=" + ctx.isNear() +
            ", ctxDht=" + ctx.isDht() + ']';

        this.entry = entry;
    }

    /**
     * @return Entry value.
     */
    @Nullable public CacheObject value() {
        return val.value();
    }

    /**
     * @return Old value.
     */
    @Nullable public CacheObject oldValue() {
        return oldVal != null ? oldVal.value() : null;
    }

    /**
     * @param oldVal Old value.
     */
    public void oldValue(CacheObject oldVal) {
        if (this.oldVal == null)
            this.oldVal = new TxEntryValueHolder();

        this.oldVal.value(op(), oldVal, true, true);
    }

    /**
     * @return {@code True} if old value present.
     */
    public boolean hasOldValue() {
        return oldVal != null && oldVal.hasValue();
    }

    /**
     * @return {@code True} if has value explicitly set.
     */
    public boolean hasValue() {
        return val.hasValue();
    }

    /**
     * @return {@code True} if has write value set.
     */
    public boolean hasWriteValue() {
        return val.hasWriteValue();
    }

    /**
     * @return {@code True} if has read value set.
     */
    public boolean hasReadValue() {
        return val.hasReadValue();
    }

    /**
     * @return Value visible for peek.
     */
    @Nullable public CacheObject previousValue() {
        return prevVal.value();
    }

    /**
     * @return {@code True} if has previous value explicitly set.
     */
    boolean hasPreviousValue() {
        return prevVal.hasValue();
    }

    /**
     * @return Previous operation to revert entry in case of filter failure.
     */
    @Nullable public GridCacheOperation previousOperation() {
        return prevVal.op();
    }

    /**
     * @return Time to live.
     */
    public long ttl() {
        return ttl;
    }

    /**
     * @param ttl Time to live.
     */
    public void ttl(long ttl) {
        this.ttl = ttl;
    }

    /**
     * @return Conflict expire time.
     */
    public long conflictExpireTime() {
        return conflictExpireTime;
    }

    /**
     * @param conflictExpireTime Conflict expire time.
     */
    public void conflictExpireTime(long conflictExpireTime) {
        this.conflictExpireTime = conflictExpireTime;
    }

    /**
     * @param val Entry value.
     * @param writeVal Write value flag.
     * @param readVal Read value flag.
     */
    public void value(@Nullable CacheObject val, boolean writeVal, boolean readVal) {
        this.val.value(this.val.op(), val, writeVal, readVal);
    }

    /**
     * Sets read value if this tx entry does not have write value yet.
     *
     * @param val Read value to set.
     */
    public void readValue(@Nullable CacheObject val) {
        this.val.value(this.val.op(), val, false, true);
    }

    /**
     * @param entryProcessor Entry processor.
     * @param invokeArgs Optional arguments for EntryProcessor.
     */
    public void addEntryProcessor(EntryProcessor<Object, Object, Object> entryProcessor, Object[] invokeArgs) {
        if (entryProcessorsCol == null)
            entryProcessorsCol = new LinkedList<>();

        entryProcessorsCol.add(new T2<>(entryProcessor, invokeArgs));

        // Must clear transform closure bytes since collection has changed.
        transformClosBytes = null;

        val.op(TRANSFORM);
    }

    /**
     * @return Collection of entry processors.
     */
    public Collection<T2<EntryProcessor<Object, Object, Object>, Object[]>> entryProcessors() {
        return entryProcessorsCol;
    }

    /**
     * @param cacheVal Value.
     * @return New value.
     */
    @SuppressWarnings("unchecked")
    public CacheObject applyEntryProcessors(CacheObject cacheVal) {
        GridCacheVersion ver;

        try {
            ver = entry.version();
        }
        catch (GridCacheEntryRemovedException ignore) {
            assert tx == null || tx.optimistic() : tx;

            ver = null;
        }

        Object val = null;
        Object keyVal = null;

        for (T2<EntryProcessor<Object, Object, Object>, Object[]> t : entryProcessors()) {
            IgniteThread.onEntryProcessorEntered(true);

            try {
                CacheInvokeEntry<Object, Object> invokeEntry = new CacheInvokeEntry(key, keyVal, cacheVal, val,
                    ver, keepBinary(), cached());

                EntryProcessor processor = t.get1();

                processor.process(invokeEntry, t.get2());

                val = invokeEntry.getValue();

                keyVal = invokeEntry.key();
            }
            catch (Exception ignore) {
                // No-op.
            }
            finally {
                IgniteThread.onEntryProcessorLeft();
            }
        }

        return ctx.toCacheObject(val);
    }

    /**
     * @param entryProcessorsCol Collection of entry processors.
     */
    public void entryProcessors(
        @Nullable Collection<T2<EntryProcessor<Object, Object, Object>, Object[]>> entryProcessorsCol) {
        this.entryProcessorsCol = entryProcessorsCol;

        // Must clear transform closure bytes since collection has changed.
        transformClosBytes = null;
    }

    /**
     * @return Cache operation.
     */
    public GridCacheOperation op() {
        return val.op();
    }

    /**
     * @param op Cache operation.
     */
    public void op(GridCacheOperation op) {
        val.op(op);
    }

    /**
     * @return {@code True} if read entry.
     */
    public boolean isRead() {
        return op() == READ;
    }

    /**
     * @param explicitVer Explicit version.
     */
    public void explicitVersion(GridCacheVersion explicitVer) {
        this.explicitVer = explicitVer;
    }

    /**
     * @return Explicit version.
     */
    public GridCacheVersion explicitVersion() {
        return explicitVer;
    }

    /**
     * @return Conflict version.
     */
    @Nullable public GridCacheVersion conflictVersion() {
        return conflictVer;
    }

    /**
     * @param conflictVer Conflict version.
     */
    public void conflictVersion(@Nullable GridCacheVersion conflictVer) {
        this.conflictVer = conflictVer;
    }

    /**
     * @return Put filters.
     */
    public CacheEntryPredicate[] filters() {
        return filters;
    }

    /**
     * @param filters Put filters.
     */
    public void filters(CacheEntryPredicate[] filters) {
        this.filters = filters;
    }

    /**
     * @return {@code True} if filters passed for fast-commit transactions.
     */
    public boolean filtersPassed() {
        return filtersPassed;
    }

    /**
     * @param filtersPassed {@code True} if filters passed for fast-commit transactions.
     */
    public void filtersPassed(boolean filtersPassed) {
        this.filtersPassed = filtersPassed;
    }

    /**
     * @return {@code True} if filters are set.
     */
    public boolean filtersSet() {
        return filtersSet;
    }

    /**
     * @param filtersSet {@code True} if filters are set and should not be replaced.
     */
    public void filtersSet(boolean filtersSet) {
        this.filtersSet = filtersSet;
    }

    /**
     * @param ctx Context.
     * @param transferExpiry {@code True} if expire policy should be marshalled.
     * @throws IgniteCheckedException If failed.
     */
    public void marshal(GridCacheSharedContext<?, ?> ctx, boolean transferExpiry) throws IgniteCheckedException {
        if (filters != null) {
            for (CacheEntryPredicate p : filters) {
                if (p != null)
                    p.prepareMarshal(this.ctx);
            }
        }

        // Do not serialize filters if they are null.
        if (transformClosBytes == null && entryProcessorsCol != null)
            transformClosBytes = CU.marshal(this.ctx, entryProcessorsCol);

        if (transferExpiry)
            transferExpiryPlc = expiryPlc != null && expiryPlc != this.ctx.expiry();

        key.prepareMarshal(context().cacheObjectContext());

        val.marshal(context());

        if (transferExpiryPlc) {
            if (expiryPlcBytes == null)
                expiryPlcBytes = CU.marshal(this.ctx, new IgniteExternalizableExpiryPolicy(expiryPlc));
        }
        else
            expiryPlcBytes = null;
    }

    /**
     * Unmarshalls entry.
     *
     * @param ctx Cache context.
     * @param near Near flag.
     * @param clsLdr Class loader.
     * @throws IgniteCheckedException If un-marshalling failed.
     */
    public void unmarshal(GridCacheSharedContext<?, ?> ctx, boolean near,
        ClassLoader clsLdr) throws IgniteCheckedException {
        if (this.ctx == null) {
            GridCacheContext<?, ?> cacheCtx = ctx.cacheContext(cacheId);

            assert cacheCtx != null : "Failed to find cache context [cacheId=" + cacheId +
                ", readyTopVer=" + ctx.exchange().readyAffinityVersion() + ']';

            if (cacheCtx.isNear() && !near)
                cacheCtx = cacheCtx.near().dht().context();
            else if (!cacheCtx.isNear() && near)
                cacheCtx = cacheCtx.dht().near().context();

            this.ctx = cacheCtx;
        }

        // Unmarshal transform closure anyway if it exists.
        if (transformClosBytes != null && entryProcessorsCol == null)
            entryProcessorsCol = U.unmarshal(ctx, transformClosBytes, U.resolveClassLoader(clsLdr, ctx.gridConfig()));

        if (filters == null)
            filters = CU.empty0();
        else {
            for (CacheEntryPredicate p : filters) {
                if (p != null)
                    p.finishUnmarshal(ctx.cacheContext(cacheId), clsLdr);
            }
        }

        key.finishUnmarshal(context().cacheObjectContext(), clsLdr);

        val.unmarshal(this.ctx, clsLdr);

        if (expiryPlcBytes != null && expiryPlc == null)
            expiryPlc = U.unmarshal(ctx, expiryPlcBytes, U.resolveClassLoader(clsLdr, ctx.gridConfig()));
    }

    /**
     * @param expiryPlc Expiry policy.
     */
    public void expiry(@Nullable ExpiryPolicy expiryPlc) {
        this.expiryPlc = expiryPlc;
    }

    /**
     * @return Expiry policy.
     */
    @Nullable public ExpiryPolicy expiry() {
        return expiryPlc;
    }

    /**
     * @return Entry processor calculated value.
     */
    public T2<GridCacheOperation, CacheObject> entryProcessorCalculatedValue() {
        return entryProcessorCalcVal;
    }

    /**
     * @param entryProcessorCalcVal Entry processor calculated value.
     */
    public void entryProcessorCalculatedValue(T2<GridCacheOperation, CacheObject> entryProcessorCalcVal) {
        assert entryProcessorCalcVal != null;

        this.entryProcessorCalcVal = entryProcessorCalcVal;
    }

    /**
     * Gets stored entry version. Version is stored for all entries in serializable transaction or
     * when value is read using {@link IgniteCache#getEntry(Object)} method.
     *
     * @return Entry version.
     */
    @Nullable public GridCacheVersion entryReadVersion() {
        return serReadVer;
    }

    /**
     * @param ver Entry version.
     */
    public void entryReadVersion(GridCacheVersion ver) {
        assert this.serReadVer == null: "Wrong version [serReadVer=" + serReadVer + ", ver=" + ver + "]";
        assert ver != null;

        this.serReadVer = ver;
    }

    /**
     * Clears recorded read version, should be done before starting commit of not serializable/optimistic transaction.
     */
    public void clearEntryReadVersion() {
        serReadVer = null;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType(), fieldsCount()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 0:
                if (!writer.writeInt("cacheId", cacheId))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeLong("conflictExpireTime", conflictExpireTime))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeMessage("conflictVer", conflictVer))
                    return false;

                writer.incrementState();

            case 3:
                if (!writer.writeByteArray("expiryPlcBytes", expiryPlcBytes))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeMessage("explicitVer", explicitVer))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeObjectArray("filters",
                    !F.isEmptyOrNulls(filters) ? filters : null, MessageCollectionItemType.MSG))
                    return false;

                writer.incrementState();

            case 6:
                if (!writer.writeByte("flags", flags))
                    return false;

                writer.incrementState();

            case 7:
                if (!writer.writeMessage("key", key))
                    return false;

                writer.incrementState();

            case 8:
                if (!writer.writeMessage("oldVal", oldVal))
                    return false;

                writer.incrementState();

            case 9:
                if (!writer.writeMessage("serReadVer", serReadVer))
                    return false;

                writer.incrementState();

            case 10:
                if (!writer.writeByteArray("transformClosBytes", transformClosBytes))
                    return false;

                writer.incrementState();

            case 11:
                if (!writer.writeLong("ttl", ttl))
                    return false;

                writer.incrementState();

            case 12:
                if (!writer.writeMessage("val", val))
                    return false;

                writer.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        reader.setBuffer(buf);

        if (!reader.beforeMessageRead())
            return false;

        switch (reader.state()) {
            case 0:
                cacheId = reader.readInt("cacheId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                conflictExpireTime = reader.readLong("conflictExpireTime");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                conflictVer = reader.readMessage("conflictVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 3:
                expiryPlcBytes = reader.readByteArray("expiryPlcBytes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 4:
                explicitVer = reader.readMessage("explicitVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 5:
                filters = reader.readObjectArray("filters", MessageCollectionItemType.MSG, CacheEntryPredicate.class);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 6:
                flags = reader.readByte("flags");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 7:
                key = reader.readMessage("key");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 8:
                oldVal = reader.readMessage("oldVal");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 9:
                serReadVer = reader.readMessage("serReadVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 10:
                transformClosBytes = reader.readByteArray("transformClosBytes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 11:
                ttl = reader.readLong("ttl");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 12:
                val = reader.readMessage("val");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(IgniteTxEntry.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 100;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 13;
    }

    /** {@inheritDoc} */
    @Override public Class<?> deployClass() {
        ClassLoader clsLdr = getClass().getClassLoader();

        CacheObject val = value();

        // First of all check classes that may be loaded by class loader other than application one.
        return key != null && !clsLdr.equals(key.getClass().getClassLoader()) ?
            key.getClass() : val != null ? val.getClass() : getClass();
    }

    /** {@inheritDoc} */
    @Override public ClassLoader classLoader() {
        return deployClass().getClassLoader();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return GridToStringBuilder.toString(IgniteTxEntry.class, this, "xidVer", tx == null ? "null" : tx.xidVersion());
    }
}
