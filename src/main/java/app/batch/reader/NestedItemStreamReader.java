package app.batch.reader;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamReader;

/**
 * Created by Liu on 9/21/2017.
 */
public class NestedItemStreamReader<K,T> implements ItemStreamReader<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(NestedItemStreamReader.class);

    // poison pill for terminating read queue
    private static final Object DONE = new Object();

    private NestedItemStreamReader<K,T> parent;

    private ItemStreamReader<T> delegate;

    private Function<T,K> keyFunction;

    private BiFunction<T,T,T> mergeFunction;

    private Collection<Function<Collection<T>, ItemStreamReader<T>>> slaveReaderProviders;

    private BlockingQueue<Object> readQueue;

    private ConcurrentMap<K,T> itemPool;

    private int batchSize = 10; // default batch size

    public NestedItemStreamReader() {

    }

    public NestedItemStreamReader(final ItemStreamReader<T> delegate) {
        this.delegate = delegate;
    }

    @Override
    public T read() throws Exception {
        if (readQueue == null) {
            throw new IllegalStateException("Read queue has not been initialized.");
        }
        T o;
        try {
            final Object t = readQueue.take();
            o = t == DONE ? null : (T) t;
        } catch (InterruptedException e) {
            o = null;
        }
        return o;
    }

    @Override
    public void open(final ExecutionContext executionContext) throws ItemStreamException {
        process(executionContext);
    }

    @Override
    public void update(final ExecutionContext executionContext) throws ItemStreamException {

    }

    @Override
    public void close() throws ItemStreamException {

    }

    private void process(final ExecutionContext executionContext) {
        Objects.requireNonNull(delegate, "Delegate item reader is not provided.");

        if (readQueue == null) {
            readQueue = new LinkedBlockingQueue<>();
        }
        if (itemPool == null) {
            itemPool = new ConcurrentHashMap<>();
        }

        final Runnable runnable = () -> {
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
                        if (keyFunction != null) {
                            // update item pool
                            final K key = keyFunction.apply(o);
                            final T s = o;
                            o = itemPool.compute(key, (k, v) -> {
                                if (v == null || mergeFunction == null) {
                                    return s;
                                } else {
                                    synchronized (v) {
                                        return mergeFunction.apply(v, s);
                                    }
                                }
                            });
                        }
                        itemBuffer.add(o);
                        if (itemBuffer.size() == batchSize) {
                            processItemBuffer(itemBuffer, executionContext);
                            if (parent == null) {
                                itemPool.clear();
                            }
                            itemBuffer.clear();
                        }
                    }
                } while (o != null);
                if (!itemBuffer.isEmpty()) {
                    processItemBuffer(itemBuffer, executionContext);
                    if (parent == null) {
                        itemPool.clear();
                    }
                    itemBuffer.clear();
                }
            } catch (Exception e) {
                LOGGER.error("{}", e);
            } finally {
                if (parent == null) {
                    itemPool.clear();
                }
                delegate.close();
                readQueue.add(DONE);
            }
        };
        if (parent == null) {
            // async for root reader
            CompletableFuture.runAsync(runnable);
        } else {
            runnable.run();
        }
    }

    public void processItemBuffer(final Collection<T> buffer, final ExecutionContext executionContext) {
        if (slaveReaderProviders != null) {
            final List<CompletableFuture<Void>> cfs = slaveReaderProviders.stream().map(provider -> {
                final ItemStreamReader<T> slaveReader = provider.apply(buffer);
                final NestedItemStreamReader<K, T> nestedSlaveReader;
                if (slaveReader instanceof NestedItemStreamReader) {
                    nestedSlaveReader = (NestedItemStreamReader<K, T>) slaveReader;
                } else {
                    nestedSlaveReader = new NestedItemStreamReader<>(delegate);
                    nestedSlaveReader.keyFunction = keyFunction;
                    nestedSlaveReader.mergeFunction = mergeFunction;
                }
                nestedSlaveReader.parent = this;
                nestedSlaveReader.itemPool = itemPool;
                return CompletableFuture.runAsync(() -> nestedSlaveReader.open(executionContext));

            }).collect(Collectors.toList());
            cfs.forEach(CompletableFuture::join);
        }
        readQueue.addAll(buffer);
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
