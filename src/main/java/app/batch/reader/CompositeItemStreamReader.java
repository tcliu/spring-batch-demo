package app.batch.reader;

import java.util.Arrays;
import java.util.Iterator;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;

/**
 * Created by Liu on 9/24/2017.
 */
public class CompositeItemStreamReader<T> extends AbstractItemCountingItemStreamItemReader<T> {

    private final Iterable<ItemStreamReader<T>> itemStreamReaders;

    private Iterator<ItemStreamReader<T>> readerItr;

    private ItemStreamReader<T> curReader;

    private ExecutionContext executionContext;

    public CompositeItemStreamReader(final ItemStreamReader<T>... itemStreamReaders) {
        this(Arrays.asList(itemStreamReaders));
    }

    public CompositeItemStreamReader(final Iterable<ItemStreamReader<T>> itemStreamReaders) {
        this.itemStreamReaders = itemStreamReaders;
        setExecutionContextName("executionContext_" + toString());
    }

    @Override
    public void open(final ExecutionContext executionContext) throws ItemStreamException {
        super.open(executionContext);
        this.executionContext = executionContext;
    }

    @Override
    protected T doRead() throws Exception {
        T o = null;
        if (itemStreamReaders != null) {
            if (readerItr == null) {
                readerItr = itemStreamReaders.iterator();
                if (readerItr.hasNext()) {
                    curReader = readerItr.next();
                }
            }
            for (boolean done = false; curReader != null && !done; ) {
                o = curReader.read();
                if (o == null && readerItr.hasNext()) {
                    curReader = readerItr.next();
                } else {
                    done = true;
                }
            }
        }
        return o;
    }

    @Override
    protected void doOpen() throws Exception {
        itemStreamReaders.forEach(reader -> reader.open(executionContext));
    }

    @Override
    protected void doClose() throws Exception {
        itemStreamReaders.forEach(ItemStreamReader::close);
    }

}
