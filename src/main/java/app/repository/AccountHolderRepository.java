package app.repository;

import app.model.AccountHolder;
import org.springframework.data.jpa.repository.JpaRepository;

/**
 * Created by Liu on 11/27/2016.
 */
public interface AccountHolderRepository extends JpaRepository<AccountHolder, Integer> {

}
