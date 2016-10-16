package app.batch.writer;

import app.exception.RetryableException;
import app.model.Person;
import app.service.PersonService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

/**
 * Created by Liu on 10/15/2016.
 */
public class PersonWriter implements ItemWriter<Person> {

    private static final Logger logger = LoggerFactory.getLogger(PersonWriter.class);

    @Autowired
    private PersonService personService;

    private int i = 0;

    @Override
    public void write(final List<? extends Person> items) throws Exception {
        if (items.size() == 10) {
            i++;
            if (i == 3) {
                throw new RetryableException("cannot write record");
            }
        }
        items.forEach(Person::beforeSave);
        personService.save(items);
        logger.info("Wrote {} items", items.size());
    }

}
