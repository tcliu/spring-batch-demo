package app.repository;

import app.model.BatchExecution;
import org.springframework.data.jpa.repository.JpaRepository;

/**
 * Created by Liu on 10/15/2016.
 */
public interface BatchExecutionRepository extends JpaRepository<BatchExecution,Integer> {
}
