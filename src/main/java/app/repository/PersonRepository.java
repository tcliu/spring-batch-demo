package app.repository;

import app.model.Person;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

/**
 * Created by Liu on 10/14/2016.
 */
@Repository
public interface PersonRepository extends JpaRepository<Person,String> {
}
