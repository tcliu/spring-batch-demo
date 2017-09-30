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
import java.util.function.BiFunction;
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

    private Function<Map<String,Object>,Object> keyFunction = m -> m.get("id");

    private BiFunction<Map<String,Object>,Map<String,Object>,Map<String,Object>> mergeFunction = (a, b) -> {
        a.putAll(b);
        return a;
    };

    @Test
    public void test() throws Exception {
        ForkableItemStreamReader<Map<String,Object>> reader = new ForkableItemStreamReader<>();

        ItemStreamReader<Map<String,Object>> delegate = new CompositeItemStreamReader<>(
            createDelegateReader("name", f -> constructList(f, 1, 100), null)
        );


        reader.setDelegate(delegate);
        reader.addSlaveReaderProvider(new ForkableItemStreamReader.Provider<>(items -> getSlaveItemStreamReader("code", items), keyFunction, mergeFunction));
        reader.addSlaveReaderProvider(new ForkableItemStreamReader.Provider<>(items -> getSlaveItemStreamReader("description", items), keyFunction, mergeFunction));
        reader.setFilter((o, ctx) -> (Integer) o.get("id") % 37 != 0);

        reader.setMapper((item, ctx) -> {
            final Collection<Map<String,Object>> c;
            if ((Integer) item.get("id") == 38) {
                c = Arrays.asList(item, item);
            } else {
                c = Collections.singletonList(item);
            }
            return c;
        });

        read(reader);
    }

    @Test
    public void testFk() throws Exception {
        ForkableItemStreamReader<Map<String,Object>> reader = new ForkableItemStreamReader<>();
        ItemStreamReader<Map<String,Object>> delegate = new CompositeItemStreamReader<>(
            createDelegateReader("fk1", f -> constructList(f, 1, 100), null),
            createDelegateReader("fk2", f -> constructList(f, 11, 200), null)
        );
        reader.setDelegate(delegate);
        reader.addSlaveReaderProvider(new ForkableItemStreamReader.Provider<>(items -> getSlaveItemStreamReader("fk1", items), m -> m.get("fk1"), mergeFunction));
        reader.addSlaveReaderProvider(new ForkableItemStreamReader.Provider<>(items -> getSlaveItemStreamReader("fk2", items), m -> m.get("fk2"), mergeFunction));

        read(reader);
    }

    private <T> void read(ItemStreamReader<T> reader) throws Exception {
        ExecutionContext executionContext = new ExecutionContext();
        try {
            reader.open(executionContext);
            Object o;
            while ((o = reader.read()) != null) {
                LOGGER.info("Read {}", o);
            }
            LOGGER.info("Done");
        } finally {
            reader.close();
        }
    }

    private ForkableItemStreamReader<Map<String,Object>> getSlaveItemStreamReader(String field, Collection<Map<String,Object>> items) {
        ForkableItemStreamReader<Map<String,Object>> reader = new ForkableItemStreamReader<>();

        int num = getNum(field);
        final Collection<Map<String,Object>> filtered = num == 0 ? items :
                items.stream().filter(m -> ((Integer) m.get("id")) % num == 0).collect(Collectors.toList());
        final Function<String,Collection<Map<String,Object>>> itemSupplier = f -> {
            Collection<Map<String,Object>> r;
            int fNum = Optional.of(field.replaceAll("[^0-9]+", "")).filter(StringUtils::isNotBlank).map(Integer::valueOf).orElse(0);
            if (f.startsWith("fk")) {
                r = filtered.stream().map(o -> {
                    Map<String, Object> m = new ConcurrentHashMap<>();
                    m.put("fk" + fNum, 100 * fNum);
                    m.put("code" + fNum, 1 + 100 * fNum);
                    return m;
                }).collect(Collectors.toSet());
            } else {
                r = filtered.stream().map(o -> {
                    int id = (Integer) o.get("id");
                    Map<String, Object> m = new ConcurrentHashMap<>();
                    m.put("id", id);
                    m.put(field, id);
                    return m;
                }).collect(Collectors.toSet());
            }
            return r;
        };
        final Predicate<Map<String,Object>> errorFilter = o -> o.get("id") != null && (Integer) o.get("id") == 99;
        reader.setDelegate(createDelegateReader(field, itemSupplier, errorFilter));

        if ("code".equals(field)) {
            Stream.of(2, 3, 5, 7).forEach(n -> {
                reader.addSlaveReaderProvider(new ForkableItemStreamReader.Provider<>(c -> getSlaveItemStreamReader("d" + n, c), keyFunction, mergeFunction));
            });
        } else if (num == 3) {
            Stream.of(11, 13).forEach(n -> {
                reader.addSlaveReaderProvider(new ForkableItemStreamReader.Provider<>(c -> getSlaveItemStreamReader("d" + n, c), keyFunction, mergeFunction));
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
                Thread.sleep(5L);
                final Map<String,Object> o = queue == null || queue.isEmpty() ? null : queue.poll();
                if (o != null && errorFilter != null && errorFilter.test(o)) {
                    throw new RuntimeException("Cannot read " + o.get("id"));
                }
                return o;
            }

        };
    }


    private Map<String,Object> construct(String field, int i) {
        Map<String, Object> m = new ConcurrentHashMap<>();
        m.put("id", i);
        m.put(field, field.startsWith("fk") ? 100 * getNum(field) : i);
        return m;
    }

    private List<Map<String,Object>> constructList(String field, int lb, int ub) {
        return IntStream.rangeClosed(lb, ub).mapToObj(n -> construct(field, n)).collect(Collectors.toList());
    }

    private int getNum(String str) {
        return Optional.of(str.replaceAll("[^0-9]+", "")).filter(StringUtils::isNotBlank).map(Integer::valueOf).orElse(0);
    }

}
