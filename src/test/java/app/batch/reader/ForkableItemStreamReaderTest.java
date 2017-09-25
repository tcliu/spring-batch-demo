package app.batch.reader;

import java.util.Collection;
import java.util.Deque;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;

/**
 * Created by Liu on 9/21/2017.
 */
public class ForkableItemStreamReaderTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(ForkableItemStreamReaderTest.class);

    @Test
    public void test() {
        ExecutionContext executionContext = new ExecutionContext();

        ForkableItemStreamReader<Integer,Map<String,Object>> reader = new ForkableItemStreamReader<>();

        ItemStreamReader<Map<String,Object>> delegate = new CompositeItemStreamReader<>(
            createDelegateReader("name", null, 1, 100),
            createDelegateReader("description", null, 101, 200)
        );

        reader.setDelegate(delegate);
        reader.addSlaveReaderProvider(items -> getSlaveItemStreamReader("code", items));
        reader.addSlaveReaderProvider(items -> getSlaveItemStreamReader("code1", items));
        reader.addSlaveReaderProvider(items -> getSlaveItemStreamReader("code2", items));
        reader.setKeyFunction(m -> (Integer) m.get("id"));
        reader.setMergeFunction((a, b) -> {
            a.putAll(b);
           return a;
        });

        try {
            reader.open(executionContext);
            Object o;
            while ((o = reader.read()) != null) {
                LOGGER.info("Read {}", o);
            }
            LOGGER.info("Done");
        } catch (Exception e) {
            LOGGER.error("Caught exception", e);
        } finally {
            reader.close();
        }
    }

    private ItemStreamReader<Map<String,Object>> getSlaveItemStreamReader(String field, Collection<Map<String,Object>> items) {
        ForkableItemStreamReader<Object,Map<String,Object>> reader = new ForkableItemStreamReader<>();
        if ("xxx".equals(field)) {
            items = items.stream().filter(m -> ((Integer) m.get("id")) % 3 == 0).collect(Collectors.toSet());
        } else if ("uuu".equals(field)) {
            items = items.stream().filter(m -> ((Integer) m.get("id")) % 9 == 0).collect(Collectors.toSet());
        }
        reader.setDelegate(createDelegateReader(field, items, 0, 0));
        reader.setKeyFunction(m -> m.get("id"));
        reader.setMergeFunction((a, b) -> {
            a.putAll(b);
            return a;
        });
        if ("code".equals(field)) {
            reader.addSlaveReaderProvider(items2 -> getSlaveItemStreamReader("xxx", items2));
        } else if ("xxx".equals(field)) {
            reader.addSlaveReaderProvider(items2 -> getSlaveItemStreamReader("uuu", items2));
        }

        return reader;
    }


    private ItemStreamReader<Map<String,Object>> createDelegateReader(final String field, Collection<Map<String,Object>> items, int lb, int ub) {
        return new ItemStreamReader<Map<String, Object>>() {

            Deque<Map<String,Object>> queue =
                    new LinkedList<>(items == null ? IntStream.rangeClosed(lb, ub).mapToObj(this::read).collect(Collectors.toList()) : items);

            @Override
            public void open(final ExecutionContext executionContext) throws ItemStreamException {

            }

            @Override
            public void update(final ExecutionContext executionContext) throws ItemStreamException {

            }

            @Override
            public void close() throws ItemStreamException {
                if (!queue.isEmpty()) {
                    throw new ItemStreamException(String.format("Not all items read. queue = %s", queue));
                }
            }

            @Override
            public Map<String, Object> read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
                Thread.sleep(10);
                return queue == null || queue.isEmpty() ? null : read(queue.poll());
            }

            private Map<String,Object> read(int i) {
                Map<String, Object> m = new ConcurrentHashMap<>();
                m.put("id", i);
                m.put(field, field.toUpperCase() + " " + i);
                return m;
            }

            private Map<String, Object> read(Map<String,Object> o) {
                Map<String, Object> m = new ConcurrentHashMap<>();
                m.put("id", o.get("id"));
                m.put(field, field.toUpperCase() + " " + o.get("id"));
                if (!"name".equals(field) && o.get("id").equals(10)) {
                    throw new RuntimeException("Cannot read 10");
                }
                return m;
            }
        };
    }

}
