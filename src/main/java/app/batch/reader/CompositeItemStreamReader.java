package app.batch.reader;

import java.util.Collection;
import java.util.Iterator;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;

/**
 * Created by Liu on 9/24/2017.
 */
public class CompositeItemStreamReader<T> implements ItemStreamReader<T> {

    private final Collection<ItemStreamReader<T>> itemStreamReaders;

    private Iterator<ItemStreamReader<T>> readerItr;

    private ItemStreamReader<T> curReader;

    public CompositeItemStreamReader(final ItemStreamReader<T>... itemStreamReaders) {
        this.itemStreamReaders = Stream.of(itemStreamReaders).collect(Collectors.toList());
    }

    public CompositeItemStreamReader(final Collection<ItemStreamReader<T>> itemStreamReaders) {
        this.itemStreamReaders = itemStreamReaders;
    }

    @Override
    public T read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
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
    public void open(final ExecutionContext executionContext) throws ItemStreamException {
        itemStreamReaders.forEach(reader -> reader.open(executionContext));
    }

    @Override
    public void update(final ExecutionContext executionContext) throws ItemStreamException {
        itemStreamReaders.forEach(reader -> reader.update(executionContext));
    }

    @Override
    public void close() throws ItemStreamException {
        itemStreamReaders.forEach(ItemStreamReader::close);
    }
}
