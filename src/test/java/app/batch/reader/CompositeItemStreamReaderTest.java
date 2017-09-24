package app.batch.reader;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * Created by Liu on 9/24/2017.
 */
public class CompositeItemStreamReaderTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(CompositeItemStreamReaderTest.class);

    @Test
    public void noReader() throws Exception {

        ItemStreamReader<Integer> r = new CompositeItemStreamReader<>(Collections.emptyList());

        List<Integer> result = new ArrayList<>();
        Integer o;
        while ((o = r.read()) != null) {
            result.add(o);
        }
        assertThat(result, is(Collections.emptyList()));

    }

    @Test
    public void singleReader() throws Exception {

        final ItemStreamReader<Integer> r1 = createItemStreamReader(1, 5);
        final ItemStreamReader<Integer> r = new CompositeItemStreamReader<>(Collections.singletonList(r1));

        final List<Integer> result = new ArrayList<>();
        Integer o;
        while ((o = r.read()) != null) {
            result.add(o);
        }
        assertThat(result, contains(1, 2, 3, 4, 5));

    }

    @Test
    public void multipleReaders() throws Exception {
        final ItemStreamReader<Integer> r1 = createItemStreamReader(1, 5);
        final ItemStreamReader<Integer> r2 = createItemStreamReader(6, 5);
        final ItemStreamReader<Integer> r3 = createItemStreamReader(12, 17);
        final ItemStreamReader<Integer> r = new CompositeItemStreamReader<>(Arrays.asList(r1, r2, r3));

        final List<Integer> result = new ArrayList<>();
        Integer o;
        while ((o = r.read()) != null) {
            result.add(o);
        }
        assertThat(result, contains(1, 2, 3, 4, 5, 12, 13, 14, 15, 16, 17));

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

}
