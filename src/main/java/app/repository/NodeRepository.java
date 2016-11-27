package app.repository;

import app.model.Node;
import org.springframework.data.jpa.repository.JpaRepository;

/**
 * Created by Liu on 11/26/2016.
 */
public interface NodeRepository extends JpaRepository<Node, Integer> {
}
