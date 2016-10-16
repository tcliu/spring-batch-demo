package app.repository;

import app.model.ApplicationLog;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * Created by Liu on 10/15/2016.
 */
@Repository
public interface ApplicationLogRepository extends JpaRepository<ApplicationLog,Integer> {

    List<ApplicationLog> findByReferenceId(Integer referenceId);

    List<ApplicationLog> findByReferenceIdAndStatus(Integer referenceId, String status);

}
