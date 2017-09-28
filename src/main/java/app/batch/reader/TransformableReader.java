package app.batch.reader;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Objects;
import java.util.Queue;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;

/**
 * Created by Liu on 9/28/2017.
 */
public class TransformableReader<T> implements ItemStreamReader<T> {

    private final ItemStreamReader<T> reader;

    private BiFunction<T, ExecutionContext, Collection<T>> mapper;

    private BiPredicate<T, ExecutionContext> filter;

    private ExecutionContext executionContext;

    private Queue<T> itemQueue;

    public TransformableReader(final ItemStreamReader<T> reader) {
        Objects.requireNonNull(reader, "Item reader is not provided.");
        this.reader = reader;
    }

    @Override
    public T read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
        final T item = reader.read();
        if (item != null && (filter == null || filter.test(item, executionContext))) {
            if (mapper == null) {
                itemQueue.add(item);
            } else {
                final Collection<T> c = mapper.apply(item, executionContext);
                itemQueue.addAll(c);
            }
        }
        return itemQueue == null || itemQueue.isEmpty() ? null : itemQueue.poll();
    }

    @Override
    public void open(final ExecutionContext executionContext) throws ItemStreamException {
        reader.open(executionContext);
        this.executionContext = executionContext;
        this.itemQueue = new LinkedList<>();
    }

    @Override
    public void update(final ExecutionContext executionContext) throws ItemStreamException {
        reader.update(executionContext);
    }

    @Override
    public void close() throws ItemStreamException {
        reader.close();
    }

    public void setMapper(final BiFunction<T, ExecutionContext, Collection<T>> mapper) {
        this.mapper = mapper;
    }

    public void setFilter(final BiPredicate<T, ExecutionContext> filter) {
        this.filter = filter;
    }
}
