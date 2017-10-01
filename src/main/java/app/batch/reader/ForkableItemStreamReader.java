package app.batch.reader;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;

/**
 * Created by Liu on 9/21/2017.
 */
public class ForkableItemStreamReader<T> extends AbstractItemCountingItemStreamItemReader<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ForkableItemStreamReader.class);

    // poison object for terminating the read queue
    private static final Object DONE = new Object();

    // properties

    private ItemStreamReader<T> delegate;

    private Function<T, ?> keyFunction;

    private BiFunction<T, T, T> mergeFunction;

    private BiFunction<T, ExecutionContext, Collection<T>> mapper;

    private BiPredicate<T, ExecutionContext> filter;

    private Collection<Provider<T>> slaveReaderProviders;

    private ExecutorService executor;

    // internal state variables

    private ForkableItemStreamReader<T> parent;

    private BlockingQueue<Object> readQueue;

    private BlockingQueue<Object> batchQueue;

    private Map<Object, Map<Object, Collection<T>>> itemPool;

    private Collection<T> mergedItems;

    private ExecutionContext executionContext;

    private Exception readException;

    private boolean hasOwnExecutor;

    private int batchSize = 100;     // default batch size

    private int maxConcurrentBatchCount = 20;

    private int readBufferSize = 1000;

    interface CheckedRunnable extends Runnable {

        void doRun() throws Exception;

        @Override
        default void run() {
            try {
                doRun();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

    }

    interface CheckedSupplier<U> extends Supplier<U> {
        U doGet() throws Exception;

        @Override
        default U get() {
            try {
                return doGet();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static class Provider<T> implements Function<Collection<T>, ForkableItemStreamReader<T>> {

        private Function<Collection<T>, ForkableItemStreamReader<T>> provider;

        private Function<T, ?> keyFunction;

        private BiFunction<T, T, T> mergeFunction;

        private boolean useInnerJoin;

        public Provider(final Function<Collection<T>, ForkableItemStreamReader<T>> provider,
                        final Function<T, ?> keyFunction,
                        final BiFunction<T, T, T> mergeFunction,
                        final boolean useInnerJoin) {
            this.provider = provider;
            this.keyFunction = keyFunction;
            this.mergeFunction = mergeFunction;
            this.useInnerJoin = useInnerJoin;
        }

        public Function<Collection<T>, ForkableItemStreamReader<T>> getProvider() {
            return provider;
        }

        public void setProvider(final Function<Collection<T>, ForkableItemStreamReader<T>> provider) {
            this.provider = provider;
        }

        public Function<T, ?> getKeyFunction() {
            return keyFunction;
        }

        public void setKeyFunction(final Function<T, ?> keyFunction) {
            this.keyFunction = keyFunction;
        }

        public BiFunction<T, T, T> getMergeFunction() {
            return mergeFunction;
        }

        public void setMergeFunction(final BiFunction<T, T, T> mergeFunction) {
            this.mergeFunction = mergeFunction;
        }

        public boolean isUseInnerJoin() {
            return useInnerJoin;
        }

        public void setUseInnerJoin(final boolean useInnerJoin) {
            this.useInnerJoin = useInnerJoin;
        }

        @Override
        public ForkableItemStreamReader<T> apply(final Collection<T> c) {
            Objects.requireNonNull(provider, "Item provider is not provided.");
            Objects.requireNonNull(keyFunction, "Key function is not provided.");
            Objects.requireNonNull(mergeFunction, "Merge function is provided.");
            final ForkableItemStreamReader<T> reader = provider.apply(c);
            reader.setKeyFunction(keyFunction);
            reader.setMergeFunction(mergeFunction);
            return reader;
        }
    }

    public ForkableItemStreamReader() {
        this(null);
    }

    public ForkableItemStreamReader(final ItemStreamReader<T> delegate) {
        this.delegate = delegate;
        setExecutionContextName("executionContext_" + toString());
    }

    @Override
    public void open(final ExecutionContext executionContext) throws ItemStreamException {
        this.executionContext = executionContext;
        super.open(executionContext);
    }

    @Override
    protected T doRead() throws Exception {
        T o;
        if (readQueue == null) {
            throw new IllegalStateException("Read queue has not been initialized.");
        } else {
            final Object t = readQueue.take();
            if (readException != null) {
                throw readException;
            }
            o = t == DONE ? null : (T) t;
        }
        return o;
    }

    @Override
    protected void doOpen() throws Exception {
        hasOwnExecutor = executor == null;
        if (executor == null) {
            executor = Executors.newCachedThreadPool();
        }
        readQueue = new LinkedBlockingQueue<>(readBufferSize);
        batchQueue = new LinkedBlockingQueue<>(maxConcurrentBatchCount);
        if (itemPool == null) {
            itemPool = new ConcurrentHashMap<>();
        }
        if (parent == null) {
            // async for root reader
            final CompletableFuture<?> f = async(this::process);
            f.whenComplete((res, e) -> {
                if (e != null) {
                    readException = (Exception) e;
                    cleanup();
                }
            });
        } else {
            process();
        }
    }

    @Override
    protected void doClose() throws Exception {

    }

    private void readItems() throws Exception {
        final List<T> itemBuffer = new ArrayList<>();
        T o;
        int batchIndex = 0;
        do {
            try {
                o = delegate.read();
            } catch (Exception e) {
                LOGGER.error("Failed to read item from {}", delegate, e);
                // read another record
                o = delegate.read();
            }
            if (o != null) {
                itemBuffer.add(o);
                if (itemBuffer.size() == batchSize) {
                    batchQueue.put(processItemBuffer(++batchIndex, itemBuffer));
                }
            }
        } while (o != null);
        if (!itemBuffer.isEmpty()) {
            // process remaining items
            batchQueue.put(processItemBuffer(++batchIndex, itemBuffer));
        }
        batchQueue.put(DONE);
    }

    private void consumeBatchQueue() throws InterruptedException {
        boolean done = false;
        while (!done) {
            final Object o = batchQueue.take();
            done = o == null || o == DONE;
            if (!done) {
                CompletableFuture<Collection<T>> cf = (CompletableFuture<Collection<T>>) o;
                Collection<T> items = cf.join();
                readQueue.addAll(items);
            }
        }
    }

    private void process() {
        Objects.requireNonNull(delegate, "Delegate item reader is not provided.");
        try {
            delegate.open(executionContext);
            async(this::readItems);
            consumeBatchQueue();
        } catch (final Exception e) {
            LOGGER.error("Failed to process item reader.", e);
        } finally {
            cleanup();
        }
    }

    private void cleanup() {
        if (hasOwnExecutor) {
            executor.shutdown();
        }
        readQueue.add(DONE);
        batchQueue.clear();
        delegate.close();
        parent = null;
        batchQueue = null;
        mergedItems = null;
        delegate = null;
    }

    /**
     * Inserts or merges an item into the item pool.
     *
     * @param o             item to insert or merge
     * @param keyFunction   the key function
     * @param mergeFunction the merge function
     * @param insertOnly    {@code true} to insert the object only (no merge operation)
     */
    private void updateItemPool(final T o,
                                final Function<T, ?> keyFunction,
                                final BiFunction<T,T,T> mergeFunction,
                                boolean insertOnly) {
        if (keyFunction != null) {
            final Object key = keyFunction == null ? null : keyFunction.apply(o);
            if (key != null) {
                // key - key function, value - map of item key and list of values
                itemPool.compute(keyFunction, (pk, pv) -> {
                    final Map<Object, Collection<T>> subPool = pv == null ? new ConcurrentHashMap<>() : pv;
                    subPool.compute(key, (k, v) -> {
                        final Collection<T> l = v == null ? ConcurrentHashMap.newKeySet() : v;
                        if (insertOnly || l.isEmpty()) {
                            l.add(o);
                        } else if (mergeFunction != null) {
                            l.forEach(u -> {
                                synchronized (u) {
                                    mergeFunction.apply(u, o);
                                }
                            });
                            if (mergedItems != null) {
                                mergedItems.addAll(l);
                            }
                        }
                        return l;
                    });
                    return subPool;
                });
            }
        }
    }

    private void clearItemPool(final Collection<T> items,
                                final Function<T, ?> keyFunction) {
        itemPool.compute(keyFunction, (pk, pv) -> {
            if (pv != null) {
                items.forEach(o -> {
                    Object key = keyFunction.apply(o);
                    if (key != null) {
                        pv.compute(key, (k, v) -> {
                            if (v != null) {
                                v.clear();
                            }
                            return v;
                        });
                        pv.remove(key);
                    }
                });
            }
            return pv;
        });
    }

    private CompletableFuture<Collection<T>> processItemBuffer(int batchIndex, final Collection<T> buffer) {
        final Collection<T> bufferCopy = new ArrayList<>(buffer);
        final CompletableFuture<Collection<T>> f = async(() -> {
            if (parent == null) {
                LOGGER.info("Processing batch {} (size = {}, queue size = {})", batchIndex, bufferCopy.size(), batchQueue.size());
            }
            final Collection<T> items = bufferCopy.stream()
                .filter(o -> filter == null || filter.test(o, executionContext))
                .flatMap(o -> mapper == null ? Stream.of(o) : mapper.apply(o, executionContext).stream())
                .collect(Collectors.toList());
            if (keyFunction != null) {
                // child reader merges items into the item pool from parent
                items.forEach(o -> updateItemPool(o, keyFunction, mergeFunction, parent == null));
            }
            if (slaveReaderProviders != null) {
                // parent creates a slave item reader for each slave reader provider
                final Collection<CompletableFuture<ForkableItemStreamReader<T>>> cfs = slaveReaderProviders.stream().map(provider -> {
                    items.forEach(o -> updateItemPool(o, provider.getKeyFunction(), provider.getMergeFunction(), parent == null));
                    final ForkableItemStreamReader<T> slaveReader = provider.apply(items);
                    slaveReader.parent = this;
                    slaveReader.executor = executor;
                    slaveReader.itemPool = itemPool;
                    if (provider.useInnerJoin) {
                        slaveReader.mergedItems = new HashSet<>();
                    }
                    return async(() -> {
                        slaveReader.open(executionContext);
                        return slaveReader;
                    });
                }).collect(Collectors.toList());
                // wait for all slave item readers to complete
                final Collection<ForkableItemStreamReader<T>> slaveReaders = cfs
                    .stream().map(CompletableFuture::join).collect(Collectors.toList());
                if (parent == null) {
                    // remove matched keys from item pool after forked item readers complete processing
                    slaveReaderProviders.stream()
                        .filter(provider -> provider.getKeyFunction() != null)
                        .map(Provider::getKeyFunction)
                        .forEach(keyFunction -> clearItemPool(items, keyFunction));
                    // remove items which are not contained in merged set
                    slaveReaders.forEach(slaveReader -> {
                        if (slaveReader.mergedItems != null) {
                            items.removeIf(o -> !slaveReader.mergedItems.contains(o));
                            slaveReader.mergedItems = null;
                        }
                    });
                }
            }
            if (parent == null) {
                LOGGER.info("Processed batch {} (size = {}, queue size = {})", batchIndex, items.size(), batchQueue.size());
            }
            return items;
        }).whenComplete((res, e) -> {
            if (e != null) {
                readException = (Exception) e;
                LOGGER.error("Encountered error", e);
                batchQueue.add(DONE);
            }
        });
        if (parent != null) {
            f.join();
        }
        buffer.clear();
        return f;
    }

    private CompletableFuture<Void> async(CheckedRunnable runnable) {
        return CompletableFuture.runAsync(runnable, executor);
    }

    private <U> CompletableFuture<U> async(CheckedSupplier<U> supplier) {
        return CompletableFuture.supplyAsync(supplier, executor);
    }

    public ItemStreamReader<T> getDelegate() {
        return delegate;
    }

    public void setDelegate(final ItemStreamReader<T> delegate) {
        this.delegate = delegate;
    }

    public void setKeyFunction(final Function<T, ?> keyFunction) {
        this.keyFunction = keyFunction;
    }

    public void setMergeFunction(final BiFunction<T, T, T> mergeFunction) {
        this.mergeFunction = mergeFunction;
    }

    public void setMapper(final BiFunction<T, ExecutionContext, Collection<T>> mapper) {
        this.mapper = mapper;
    }

    public void setFilter(final BiPredicate<T, ExecutionContext> filter) {
        this.filter = filter;
    }

    public void setBatchSize(final int batchSize) {
        this.batchSize = batchSize;
    }

    public void setExecutor(final ExecutorService executor) {
        this.executor = executor;
    }

    public void setSlaveReaderProviders(final Collection<Provider<T>> slaveReaderProviders) {
        this.slaveReaderProviders = slaveReaderProviders;
    }

    public void addSlaveReaderProvider(final Provider<T> slaveReaderProvider) {
        if (slaveReaderProviders == null) {
            slaveReaderProviders = new ArrayList<>();
        }
        slaveReaderProviders.add(slaveReaderProvider);
    }

}
