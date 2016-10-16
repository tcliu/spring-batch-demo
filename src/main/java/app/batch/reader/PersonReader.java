package app.batch.reader;

import app.model.Person;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.batch.item.support.AbstractItemStreamItemReader;

import java.time.LocalDate;

/**
 * Created by Liu on 10/15/2016.
 */
public class PersonReader extends AbstractItemStreamItemReader<Person> {

    private int i;

    @Override
    public Person read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
        i++;
        Person person = null;
        if (i <= 50) {
            person = new Person(null, "Hello" + i, "world" + i);
            person.setDateOfBirth(LocalDate.now());
            person.setNationalityCode(i == 10 ? "Singapore" : "Hong Kong");
        }
        return person;
    }
}
