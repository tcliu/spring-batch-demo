package app.batch.reader;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;

/**
 * Created by Liu on 9/21/2017.
 */
public class ForkableItemStreamReader<T,K> extends AbstractItemCountingItemStreamItemReader<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ForkableItemStreamReader.class);

    // poison object for terminating the read queue
    private static final Object DONE = new Object();

    private ForkableItemStreamReader<T,K> parent;

    private ItemStreamReader<T> delegate;

    private Function<T,K> keyFunction;

    private BiFunction<T,T,T> mergeFunction;

    private Collection<Function<Collection<T>, ItemStreamReader<T>>> slaveReaderProviders;

    private BlockingQueue<Object> readQueue;

    private BlockingQueue<Object> batchQueue;

    private CountDownLatch doneLatch;

    private ConcurrentMap<K,T> itemPool;

    private ExecutionContext executionContext;

    private ExecutorService executor;

    private boolean ownedExecutor;

    private int batchSize = 20;     // default batch size

    private int maxConcurrentBatchCount = 20;

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
        if (readQueue == null) {
            throw new IllegalStateException("Read queue has not been initialized.");
        }
        final Object t = readQueue.take();
        return t == DONE ? null : (T) t;
    }

    @Override
    protected void doOpen() throws Exception {
        ownedExecutor = executor == null;
        if (executor == null) {
            executor = Executors.newCachedThreadPool();
        }
        readQueue = new LinkedBlockingQueue<>();
        batchQueue = new LinkedBlockingQueue<>(maxConcurrentBatchCount);
        doneLatch = new CountDownLatch(1);
        if (itemPool == null) {
            itemPool = new ConcurrentHashMap<>();
        }
        if (parent == null) {
            // async for root reader
            async(this::process);
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
                    batchQueue.put(processItemBuffer(++batchIndex, itemBuffer, false));
                }
            }
        } while (o != null);
        // process remaining items
        batchQueue.put(processItemBuffer(++batchIndex, itemBuffer, true));
    }

    private void consumeBatchQueue() throws InterruptedException {
        boolean done = false;
        for (; !done ;) {
            final Object o = batchQueue.take();
            done = o == null || o == DONE;
            if (done) {
                doneLatch.countDown();
            } else {
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
            async(() -> readItems());
            async(() -> consumeBatchQueue());
            doneLatch.await();
        } catch (final Exception e) {
            LOGGER.error("Failed to process item reader.", e);
        } finally {
            cleanup();
        }
    }

    private void cleanup() {
        if (parent == null) {
            itemPool.clear();
        }
        if (ownedExecutor) {
            executor.shutdown();
        }
        readQueue.add(DONE);
        batchQueue.clear();
        delegate.close();
    }

    private T processItem(final T o) {
        T item = o;
        if (keyFunction != null) {
            // update item pool
            final K key = keyFunction.apply(o);
            if (key != null) {
                item = itemPool.compute(key, (k, v) -> {
                    final T r;
                    if (v == null) {
                        r = o;
                    } else if (mergeFunction == null) {
                        r = v;
                    } else {
                        synchronized (v) {
                            r = mergeFunction.apply(v, o);
                        }
                    }
                    return r;
                });
            }
        }
        return item;
    }

    private CompletableFuture<Collection<T>> processItemBuffer(int batchIndex, final Collection<T> buffer, boolean isLast) {
        final Collection<T> processed = buffer.stream().map(this::processItem).collect(Collectors.toList());
        final CompletableFuture<Collection<T>> f = async(() -> {
            if (parent == null) {
                LOGGER.info("Processing batch {} (size = {}, queue size = {}) {}", batchIndex, processed.size(), batchQueue.size(), processed);
            }
            if (slaveReaderProviders != null) {
                final Collection<CompletableFuture<Void>> cfs = slaveReaderProviders.stream().map(provider -> {
                    final ItemStreamReader<T> slaveReader = provider.apply(processed);
                    final ForkableItemStreamReader<T, K> forkableSlaveReader;
                    if (slaveReader instanceof ForkableItemStreamReader) {
                        forkableSlaveReader = (ForkableItemStreamReader<T, K>) slaveReader;
                    } else {
                        forkableSlaveReader = new ForkableItemStreamReader<>(delegate);
                    }
                    forkableSlaveReader.parent = this;
                    forkableSlaveReader.itemPool = itemPool;
                    forkableSlaveReader.executor = executor;
                    if (forkableSlaveReader.keyFunction == null) {
                        forkableSlaveReader.keyFunction = keyFunction;
                    }
                    if (forkableSlaveReader.mergeFunction == null) {
                        forkableSlaveReader.mergeFunction = mergeFunction;
                    }
                    return async(() -> forkableSlaveReader.open(executionContext));
                }).collect(Collectors.toList());
                cfs.forEach(CompletableFuture::join);
            }
            if (parent == null) {
                if (keyFunction != null) {
                    processed.stream().forEach(item -> itemPool.remove(keyFunction.apply(item)));
                }
                LOGGER.info("Processed batch {} (size = {}) {}", batchIndex, processed.size(), processed);
            }
            if (isLast) {
                batchQueue.put(DONE);
            }
            return processed;
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

    public void setKeyFunction(final Function<T, K> keyFunction) {
        this.keyFunction = keyFunction;
    }

    public void setMergeFunction(final BiFunction<T, T, T> mergeFunction) {
        this.mergeFunction = mergeFunction;
    }

    public void setSlaveReaderProviders(final Collection<Function<Collection<T>, ItemStreamReader<T>>> slaveReaderProviders) {
        this.slaveReaderProviders = slaveReaderProviders;
    }

    public void setBatchSize(final int batchSize) {
        this.batchSize = batchSize;
    }

    public void addSlaveReaderProvider(final Function<Collection<T>, ItemStreamReader<T>> slaveReaderProvider) {
        if (slaveReaderProviders == null) {
            slaveReaderProviders = new ArrayList<>();
        }
        slaveReaderProviders.add(slaveReaderProvider);
    }

}
