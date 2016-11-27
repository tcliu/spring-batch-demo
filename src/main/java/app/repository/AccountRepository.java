package app.repository;

import app.model.Account;
import org.springframework.data.jpa.repository.JpaRepository;

/**
 * Created by Liu on 11/27/2016.
 */
public interface AccountRepository extends JpaRepository<Account, Integer> {


}
