package app.batch.reader;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
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

        ForkableItemStreamReader<Map<String,Object>,Integer> reader = new ForkableItemStreamReader<>();

        ItemStreamReader<Map<String,Object>> delegate = new CompositeItemStreamReader<>(
            createDelegateReader("name", f -> constructList(f, 1, 1000), null)
        );

        reader.setDelegate(delegate);
        reader.addSlaveReaderProvider(items -> getSlaveItemStreamReader("code", items));
        reader.addSlaveReaderProvider(items -> getSlaveItemStreamReader("description", items));
        reader.setKeyFunction(m -> (Integer) m.get("id"));
        reader.setMergeFunction((a, b) -> {
            a.putAll(b);
           return a;
        });
        reader.setFilter((item, ctx) -> (Integer) item.get("id") % 37 != 0);
        reader.setMapper((item, ctx) -> {
            final Collection<Map<String,Object>> c;
            if ((Integer) item.get("id") == 38) {
                c = Arrays.asList(item, item);
            } else {
                c = Collections.singletonList(item);
            }
            return c;
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
        ForkableItemStreamReader<Map<String,Object>,Object> reader = new ForkableItemStreamReader<>();
        reader.setBatchSize(10);
        int num = Optional.of(field.replaceAll("[^0-9]+", "")).filter(StringUtils::isNotBlank).map(Integer::valueOf).orElse(0);
        final Collection<Map<String,Object>> filtered = num == 0 ? items :
                items.stream().filter(m -> ((Integer) m.get("id")) % num == 0).collect(Collectors.toList());
        reader.setDelegate(createDelegateReader(field, f -> filtered, o -> (Integer) o.get("id") == 999));

        if ("code".equals(field)) {
            Stream.of(2, 3, 5, 7).forEach(n -> {
                reader.addSlaveReaderProvider(c -> getSlaveItemStreamReader("d" + n, c));
            });
        } else if (num == 3) {
            Stream.of(11, 13).forEach(n -> {
                reader.addSlaveReaderProvider(c -> getSlaveItemStreamReader("d" + n, c));
            });
        }

        return reader;
    }

    private ItemStreamReader<Map<String,Object>> createDelegateReader(final String field,
                                                                      final Function<String,Collection<Map<String,Object>>> itemSupplier,
                                                                      final Predicate<Map<String,Object>> errorFilter) {
        return new ItemStreamReader<Map<String, Object>>() {

            Queue<Map<String,Object>> queue;

            @Override
            public void open(final ExecutionContext executionContext) throws ItemStreamException {
                queue = new LinkedList<>(itemSupplier.apply(field));
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
                return queue == null || queue.isEmpty() ? null : read(queue.poll());
            }

            private Map<String, Object> read(Map<String,Object> o) throws InterruptedException {
                Thread.sleep(5L);
                Map<String, Object> m = new ConcurrentHashMap<>();
                m.put("id", o.get("id"));
                m.put(field, o.get("id"));
                if (errorFilter != null && errorFilter.test(o)) {
                    throw new RuntimeException("Cannot read " + o.get("id"));
                }
                return m;
            }
        };
    }


    private Map<String,Object> construct(String field, int i) {
        Map<String, Object> m = new ConcurrentHashMap<>();
        m.put("id", i);
        m.put(field, i);
        return m;
    }

    private List<Map<String,Object>> constructList(String field, int lb, int ub) {
        return IntStream.rangeClosed(lb, ub).mapToObj(n -> construct(field, n)).collect(Collectors.toList());
    }


}
