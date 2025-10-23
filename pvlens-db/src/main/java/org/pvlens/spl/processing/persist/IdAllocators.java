package org.pvlens.spl.processing.persist;

/*
 * This file is part of PVLens.
 *
 * Copyright (C) 2025 GlaxoSmithKline
 *
 * PVLens is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * PVLens is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with PVLens.  If not, see <https://www.gnu.org/licenses/>.
 */

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Global, thread-safe ID allocator and de-duplication state that persists for the
 * entire JVM run. Use {@link #getInstance()} for defaults, or
 * {@link #bootstrapIfNeeded(int, int, int, int, int)} to seed counters once.
 *
 * <pre>
 * // Example (seed from DB maxima + 1; otherwise rely on defaults = 100)
 * IdAllocators.bootstrapIfNeeded(
 *     maxProductId + 1,
 *     maxNdcId + 1,
 *     maxSplSrcId + 1,
 *     maxProductAeId + 1,
 *     maxProductIndId + 1
 * );
 *
 * IdAllocators ids = IdAllocators.getInstance();
 * int ndcId = ids.nextNdcId();
 * </pre>
 */
public final class IdAllocators {

    // ---------- Singleton plumbing ----------
    private static final int DEFAULT_START_ID = 100;
    private static volatile IdAllocators INSTANCE;

    /** Create the singleton if it doesn't exist; defaults all counters to {@value #DEFAULT_START_ID}. */
    public static IdAllocators getInstance() {
        IdAllocators ref = INSTANCE;
        if (ref == null) {
            synchronized (IdAllocators.class) {
                ref = INSTANCE;
                if (ref == null) {
                    INSTANCE = ref = new IdAllocators(
                        DEFAULT_START_ID, DEFAULT_START_ID, DEFAULT_START_ID, DEFAULT_START_ID, DEFAULT_START_ID
                    );
                }
            }
        }
        return ref;
    }

    /**
     * One-time bootstrap with custom starting values (e.g., DB maxima + 1).
     * If the singleton already exists, this call is a no-op.
     */
    public static IdAllocators bootstrapIfNeeded(int startProductId,
                                                 int startNdcId,
                                                 int startSplSrcId,
                                                 int startProductAeId,
                                                 int startProductIndId) {
        IdAllocators ref = INSTANCE;
        if (ref == null) {
            synchronized (IdAllocators.class) {
                ref = INSTANCE;
                if (ref == null) {
                    INSTANCE = ref = new IdAllocators(
                        atLeast(startProductId,   DEFAULT_START_ID),
                        atLeast(startNdcId,       DEFAULT_START_ID),
                        atLeast(startSplSrcId,    DEFAULT_START_ID),
                        atLeast(startProductAeId, DEFAULT_START_ID),
                        atLeast(startProductIndId,DEFAULT_START_ID)
                    );
                }
            }
        }
        return ref;
    }

    /**
     * If larger maxima are discovered later (e.g., after scanning historical SQL),
     * bump counters up atomically without recreating the singleton.
     */
    public static void bumpToAtLeast(Integer productNext,
                                     Integer ndcNext,
                                     Integer splSrcNext,
                                     Integer productAeNext,
                                     Integer productIndNext) {
        IdAllocators ids = getInstance();
        if (productNext    != null) bump(ids.productId,     productNext);
        if (ndcNext        != null) bump(ids.ndcId,         ndcNext);
        if (splSrcNext     != null) bump(ids.splSrcId,      splSrcNext);
        if (productAeNext  != null) bump(ids.productAeId,   productAeNext);
        if (productIndNext != null) bump(ids.productIndId,  productIndNext);
    }

    private static int atLeast(int value, int floor) {
        return Math.max(value, floor);
    }
    private static void bump(AtomicInteger ai, int targetNext) {
        while (true) {
            int cur = ai.get();
            if (cur >= targetNext) return;
            if (ai.compareAndSet(cur, targetNext)) return;
        }
    }

    // ---------- Instance state ----------
    public final AtomicInteger productId;
    public final AtomicInteger ndcId;
    public final AtomicInteger splSrcId;
    public final AtomicInteger productAeId;
    public final AtomicInteger productIndId;

    /** Tracks emitted NDC rows, key = {@code ndcCode + "|" + productNameHash} (or your chosen key). */
    public final ConcurrentMap<String, Integer> ndcIds = new ConcurrentHashMap<>();

    /** Canonical NDC_ID by NDC code (first seen). */
    public final ConcurrentMap<String, Integer> ndcByCode = new ConcurrentHashMap<>();

    // Private constructor — use getInstance()/bootstrapIfNeeded()
    private IdAllocators(int startProductId,
                         int startNdcId,
                         int startSplSrcId,
                         int startProductAeId,
                         int startProductIndId) {
        this.productId     = new AtomicInteger(startProductId);
        this.ndcId         = new AtomicInteger(startNdcId);
        this.splSrcId      = new AtomicInteger(startSplSrcId);
        this.productAeId   = new AtomicInteger(startProductAeId);
        this.productIndId  = new AtomicInteger(startProductIndId);
    }

    // ---------- Allocation helpers ----------
    public int nextProductId()    { return productId.getAndIncrement(); }
    public int nextNdcId()        { return ndcId.getAndIncrement(); }
    public int nextSplSrcId()     { return splSrcId.getAndIncrement(); }
    public int nextProductAeId()  { return productAeId.getAndIncrement(); }
    public int nextProductIndId() { return productIndId.getAndIncrement(); }

    // ---------- Test/reset hook ----------
    /** For tests only. Do not call in production runs. */
    static void _resetForTests() {
        synchronized (IdAllocators.class) { INSTANCE = null; }
    }
}
