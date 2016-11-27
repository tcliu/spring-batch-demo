package app.batch.writer;

import java.util.List;
import app.model.Account;
import org.springframework.batch.item.ItemWriter;

/**
 * Created by Liu on 11/27/2016.
 */
public class AccountWriter implements ItemWriter<Account> {

    @Override
    public void write(final List<? extends Account> items) throws Exception {
        System.out.println(items);
    }
}
