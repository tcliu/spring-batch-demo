package app.repository;

import app.model.Person;
import org.springframework.data.jpa.repository.JpaRepository;

/**
 * Created by Liu on 10/14/2016.
 */
public interface PersonRepository extends JpaRepository<Person,String> {
}
