package app.batch.processor;

import app.exception.ApplicationException;
import app.exception.RetryableException;
import app.model.Person;
import org.springframework.batch.item.ItemProcessor;

/**
 * Created by Liu on 10/15/2016.
 */
public class PersonProcessor implements ItemProcessor<Person,Person> {

    private int i = 0;

    @Override
    public Person process(final Person person) throws Exception {
        if ("world13".equals(person.getLastName())) {
            throw new RetryableException("Process error for " + person.getLastName());
        } else if ("world27".equalsIgnoreCase(person.getLastName())) {
            throw new ApplicationException("Process error for " + person.getLastName());
        }
        return person;
    }
}
