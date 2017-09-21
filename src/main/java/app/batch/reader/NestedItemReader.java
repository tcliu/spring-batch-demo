package app.batch.reader;

import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;
import io.reactivex.Observable;
import io.reactivex.schedulers.Schedulers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamReader;

/**
 * Created by Liu on 9/21/2017.
 */
public class NestedItemReader<T> implements ItemStreamReader<T> {

    private enum State {

        NEW, OPENED, INITIALIZED, COMPLETED, ERROR, CLOSED

    }

    private static final Logger LOGGER = LoggerFactory.getLogger(NestedItemReader.class);

    private ItemStreamReader<T> delegate;

    private Function<Collection<T>, ItemStreamReader<T>> slaveReaderProvider;

    private Function<T,Object> keyFunction;

    private BiFunction<T,T,T> mergeFunction;

    private BlockingQueue<T> processedQueue;

    private AtomicInteger recordCount;

    private int readCount;

    private ExecutionContext executionContext;

    private ConcurrentMap<Object,T> itemPool;

    private int batchSize = 10;

    private State state = State.NEW;

    @Override
    public T read() throws Exception {
        final Thread mainThread = Thread.currentThread();
        if (state.compareTo(State.INITIALIZED) < 0) {
            if (slaveReaderProvider != null) {
                Objects.requireNonNull(keyFunction, "Key function is null.");
                Objects.requireNonNull(mergeFunction, "Merge function is null.");
            }
            Observable<T> observable = Observable.create(emitter -> {
                T o;
                while ((o = delegate.read()) != null) {
                    if (keyFunction != null) {
                        final Object key = keyFunction.apply(o);
                        itemPool.put(key, o);
                    }
                    emitter.onNext(o);
                    recordCount.incrementAndGet();
                }
                emitter.onComplete();
            });
            observable.subscribeOn(Schedulers.io()).buffer(batchSize).subscribe(objs -> {
                if (slaveReaderProvider == null) {
                    processedQueue.addAll(objs);
                } else {
                    ItemStreamReader<T> slaveReader = slaveReaderProvider.apply(objs);
                    processSlaveReader(slaveReader);
                }
            }, e -> {
                LOGGER.error("Unable to read item.", e);
                state = State.ERROR;
                mainThread.interrupt();
            }, () -> {
                state = State.COMPLETED;
            });
            state = State.INITIALIZED;
        }
        final T o;
        boolean doRead = state == State.INITIALIZED || (state == State.COMPLETED && readCount < recordCount.get());
        if (doRead) {
            o = processedQueue.take();
            ++readCount;
        } else {
            o = null;
        }
        return o;
    }

    @Override
    public void open(final ExecutionContext executionContext) throws ItemStreamException {
        this.executionContext = executionContext;
        processedQueue = new LinkedBlockingQueue<>();
        itemPool = new ConcurrentHashMap<>();
        recordCount = new AtomicInteger(0);
        if (delegate != null) {
            delegate.open(executionContext);
        }
        state = State.OPENED;
    }

    @Override
    public void update(final ExecutionContext executionContext) throws ItemStreamException {
        if (delegate != null) {
            delegate.update(executionContext);
        }
    }

    @Override
    public void close() throws ItemStreamException {
        if (processedQueue != null) {
            processedQueue.clear();
        }
        if (delegate != null) {
            delegate.close();
        }
        state = State.CLOSED;
    }

    private void processSlaveReader(ItemStreamReader<T> slaveReader) throws Exception {
        T o;
        try {
            slaveReader.open(executionContext);
            while ((o = slaveReader.read()) != null) {
                final Object key = keyFunction.apply(o);
                final T item = itemPool.get(key);
                if (item != null) {
                    o = mergeFunction.apply(item, o);
                    itemPool.remove(key);
                }
                processedQueue.put(o);
            }
        } finally {
            slaveReader.close();
        }
    }

    public void setDelegate(final ItemStreamReader<T> delegate) {
        this.delegate = delegate;
    }

    public void setSlaveReaderProvider(Function<Collection<T>, ItemStreamReader<T>> slaveReaderProvider) {
        this.slaveReaderProvider = slaveReaderProvider;
    }

    public void setKeyFunction(final Function<T, Object> keyFunction) {
        this.keyFunction = keyFunction;
    }

    public void setMergeFunction(final BiFunction<T, T, T> mergeFunction) {
        this.mergeFunction = mergeFunction;
    }

    public void setBatchSize(final int batchSize) {
        this.batchSize = batchSize;
    }
}
