package app.repository;

import app.model.Position;
import org.springframework.data.jpa.repository.JpaRepository;

/**
 * Created by Liu on 11/27/2016.
 */
public interface PositionRepository extends JpaRepository<Position, Integer> {

}
