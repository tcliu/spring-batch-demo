package app.repository;

import app.model.ApplicationLog;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.stream.Stream;

/**
 * Created by Liu on 10/15/2016.
 */
public interface ApplicationLogRepository extends JpaRepository<ApplicationLog,Integer> {

    Stream<ApplicationLog> findByReferenceId(Integer referenceId);

    Stream<ApplicationLog> findByReferenceIdAndStatus(Integer referenceId, String status);

}
