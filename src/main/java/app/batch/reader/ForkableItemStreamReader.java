package app.batch.reader;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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
public class ForkableItemStreamReader<K,T> extends AbstractItemCountingItemStreamItemReader<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ForkableItemStreamReader.class);

    // poison object for terminating the read queue
    private static final Object DONE = new Object();

    private ForkableItemStreamReader<K,T> parent;

    private ItemStreamReader<T> delegate;

    private Function<T,K> keyFunction;

    private BiFunction<T,T,T> mergeFunction;

    private Collection<Function<Collection<T>, ItemStreamReader<T>>> slaveReaderProviders;

    private BlockingQueue<Object> readQueue;

    private ConcurrentMap<K,T> itemPool;

    private ExecutionContext executionContext;

    private ExecutorService executor;

    private int parallelism = 8;    // executor parallelism

    private int batchSize = 20;     // default batch size

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
        executor = Executors.newWorkStealingPool(parallelism);
        readQueue = new LinkedBlockingQueue<>();
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
        // do nothing
    }

    private void process() {
        Objects.requireNonNull(delegate, "Delegate item reader is not provided.");
        final Queue<CompletableFuture<Collection<T>>> q = new LinkedList<>();
        final List<T> itemBuffer = new ArrayList<>();
        T o;
        try {
            delegate.open(executionContext);
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
                        q.add(processItemBuffer(itemBuffer));
                    }
                }
            } while (o != null);
            if (!itemBuffer.isEmpty()) {
                // process remaining items
                q.add(processItemBuffer(itemBuffer));
            }
        } catch (final Exception e) {
            LOGGER.error("Failed to process item reader.", e);
        } finally {
            if (parent == null) {
                while (!q.isEmpty()) {
                    final Collection<T> items = q.poll().join();
                    readQueue.addAll(items);
                }
                itemPool.clear();
            }
            readQueue.add(DONE);
            delegate.close();
            executor.shutdown();
        }
    }

    private T processItem(final T o) {
        T item = o;
        if (keyFunction != null) {
            // update item pool
            final K key = keyFunction.apply(o);
            if (key != null) {
                item = itemPool.compute(key, (k, v) -> {
                    final T r;
                    if (v == null || mergeFunction == null) {
                        r = o;
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

    private CompletableFuture<Collection<T>> processItemBuffer(final Collection<T> buffer) {
        // LOGGER.info("Processing items {}", buffer);
        final Collection<T> processed = buffer.stream().map(this::processItem).collect(Collectors.toList());
        buffer.clear();
        final CompletableFuture<Collection<T>> f = async(() -> {
            if (slaveReaderProviders != null) {
                final List<CompletableFuture<Void>> cfs = slaveReaderProviders.stream().map(provider -> {
                    final ItemStreamReader<T> slaveReader = provider.apply(processed);
                    final ForkableItemStreamReader<K, T> forkableSlaveReader;
                    if (slaveReader instanceof ForkableItemStreamReader) {
                        forkableSlaveReader = (ForkableItemStreamReader<K, T>) slaveReader;
                    } else {
                        forkableSlaveReader = new ForkableItemStreamReader<>(delegate);
                        forkableSlaveReader.keyFunction = keyFunction;
                        forkableSlaveReader.mergeFunction = mergeFunction;
                    }
                    forkableSlaveReader.parent = this;
                    forkableSlaveReader.itemPool = itemPool;
                    return async(() -> forkableSlaveReader.open(executionContext));
                }).collect(Collectors.toList());
                cfs.forEach(CompletableFuture::join);
            }
            if (parent == null && keyFunction != null) {
                processed.stream().forEach(item -> itemPool.remove(keyFunction.apply(item)));
            }
            return processed;
        });
        if (parent != null) {
            f.join();
        }
        return f;
    }

    private CompletableFuture<Void> async(Runnable runnable) {
        return CompletableFuture.runAsync(runnable, executor);
    }

    private <U> CompletableFuture<U> async(Supplier<U> supplier) {
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

    public void setParallelism(final int parallelism) {
        this.parallelism = parallelism;
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
