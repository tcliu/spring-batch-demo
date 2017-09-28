package app.batch.reader;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;

import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertThat;

/**
 * Created by Liu on 9/28/2017.
 */
public class TransformableItemReaderTest {

    @Test
    public void transform_noInput() throws Exception {
        final ExecutionContext context = new ExecutionContext();
        ItemStreamReader<Integer> r1 = createItemStreamReader(1, 5);
        TransformableReader<Integer> t1 = new TransformableReader<>(r1);
        List<Integer> r = read(t1, context);
        assertThat(r, contains(1, 2, 3, 4, 5));
    }

    @Test
    public void transform_twoRowsPerItem() throws Exception {
        final ExecutionContext context = new ExecutionContext();
        ItemStreamReader<Integer> r1 = createItemStreamReader(1, 5);
        TransformableReader<Integer> t1 = new TransformableReader<>(r1);
        t1.setMapper((n, ctx) -> Arrays.asList(n, n * 2));
        List<Integer> r = read(t1, context);
        assertThat(r, contains(1, 2, 2, 4, 3, 6, 4, 8, 5, 10));
    }

    @Test
    public void transform_filtered() throws Exception {
        final ExecutionContext context = new ExecutionContext();
        ItemStreamReader<Integer> r1 = createItemStreamReader(1, 5);
        TransformableReader<Integer> t1 = new TransformableReader<>(r1);
        t1.setFilter((n, ctx) -> n % 2 == 1);
        List<Integer> r = read(t1, context);
        assertThat(r, contains(1, 3, 5));
    }

    @Test
    public void transform_twoRowsPerItemWithFilter() throws Exception {
        final ExecutionContext context = new ExecutionContext();
        ItemStreamReader<Integer> r1 = createItemStreamReader(1, 5);
        TransformableReader<Integer> t1 = new TransformableReader<>(r1);
        t1.setMapper((n, ctx) -> Arrays.asList(n, n * 2));
        t1.setFilter((n, ctx) -> n % 2 == 0);
        List<Integer> r = read(t1, context);
        assertThat(r, contains(2, 2, 4, 6, 4, 8, 10));
    }

    private ItemStreamReader<Integer> createItemStreamReader(int lb, int ub) {
        return new ItemStreamReader<Integer>() {

            private int cur = lb;

            @Override
            public Integer read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
                return cur <= ub ? cur++ : null;
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

    private <T> List<T> read(ItemStreamReader<T> reader, ExecutionContext executionContext) throws Exception {
        final List<T> result = new ArrayList<>();
        try {
            reader.open(executionContext);
            T o;
            while ((o = reader.read()) != null) {
                result.add(o);
            }
        } finally {
            reader.close();
        }
        return result;
    }

}
