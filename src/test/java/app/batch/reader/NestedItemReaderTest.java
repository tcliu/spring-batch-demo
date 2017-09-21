package app.batch.reader;

import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;

import static org.mockito.Mockito.mock;

/**
 * Created by Liu on 9/21/2017.
 */
public class NestedItemReaderTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(NestedItemReaderTest.class);

    @Test
    public void test() {
        ExecutionContext executionContext = mock(ExecutionContext.class);

        ItemStreamReader<Map<String,Object>> delegate = createDelegateReader();

        NestedItemReader<Map<String,Object>> reader = new NestedItemReader<>();
        reader.setDelegate(delegate);
        reader.setSlaveReaderProvider(items -> getSlaveItemStreamReader("code", items));
        reader.setKeyFunction(m -> m.get("id"));
        reader.setMergeFunction((a, b) -> {
           a.putAll(b);
           return a;
        });
        reader.setBatchSize(20);

        try {
            reader.open(executionContext);
            Object o;
            while ((o = reader.read()) != null) {
                LOGGER.info("Read {}", o);
            }
        } catch (Exception e) {
            LOGGER.error("Caught exception", e);
        } finally {
            reader.close();
        }
    }

    private NestedItemReader<Map<String,Object>> getSlaveItemStreamReader(String field, Collection<Map<String,Object>> items) {
        NestedItemReader<Map<String,Object>> reader = new NestedItemReader<>();
        if ("xxx".equals(field)) {
            items = items.stream().filter(m -> ((Integer) m.get("id")) % 3 == 0).collect(Collectors.toSet());
        }
        reader.setDelegate(createDelegateReader(field, items));
        reader.setKeyFunction(m -> m.get("id"));
        reader.setMergeFunction((a, b) -> {
            a.putAll(b);
            return a;
        });
        if ("code".equals(field)) {
            reader.setSlaveReaderProvider(items2 -> getSlaveItemStreamReader("xxx", items2));
        }
        return reader;
    }


    private ItemStreamReader<Map<String,Object>> createDelegateReader() {
        return new ItemStreamReader<Map<String, Object>>() {

            int i = 0;

            @Override
            public Map<String, Object> read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
                if (++i < 100) {
                    Map<String,Object> m = new LinkedHashMap<>();
                    m.put("id", i);
                    m.put("name", "Name " + i);
                    return m;
                }
                return null;
            }

            @Override
            public void open(final ExecutionContext executionContext) throws ItemStreamException {

            }

            @Override
            public void update(final ExecutionContext executionContext) throws ItemStreamException {

            }

            @Override
            public void close() throws ItemStreamException {

            }
        };
    }


    private ItemStreamReader<Map<String,Object>> createDelegateReader(String field, Collection<Map<String,Object>> items) {
        Deque<Map<String,Object>> queue = new LinkedList<>(items);
        return new ItemStreamReader<Map<String, Object>>() {

            @Override
            public void open(final ExecutionContext executionContext) throws ItemStreamException {

            }

            @Override
            public void update(final ExecutionContext executionContext) throws ItemStreamException {

            }

            @Override
            public void close() throws ItemStreamException {

            }

            @Override
            public Map<String, Object> read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
                return queue == null || queue.isEmpty() ? null : read(queue.poll());
            }

            private Map<String, Object> read(Map<String,Object> o) {
                Map<String, Object> m = new HashMap<>(o);
                m.remove("name");
                m.put(field, field.toUpperCase() + " " + m.get("id"));
                return m;
            }
        };
    }

}
