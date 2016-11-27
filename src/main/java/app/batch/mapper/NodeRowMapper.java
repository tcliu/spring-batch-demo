package app.batch.mapper;

import java.util.ArrayList;
import app.model.Node;
import org.springframework.jdbc.support.rowset.SqlRowSet;

/**
 * Created by Liu on 11/27/2016.
 */
public class NodeRowMapper implements StructuredItemRowMapper<Node> {

    @Override
    public boolean isNewItem(final Node item, final SqlRowSet rs, final int recordIndex) {
        return item.getId() != rs.getInt("ID1");
    }

    @Override
    public Node newItem(final SqlRowSet rs, final int recordIndex) {
        Node node = new Node();
        node.setId(rs.getInt("ID1"));
        node.setName(rs.getString("NAME1"));
        node.setType(rs.getString("TYPE1"));
        node.setValue(rs.getDouble("VALUE1"));
        node.setChildren(new ArrayList<>());
        return node;
    }

    @Override
    public void updateItem(Node item, SqlRowSet rs, final int recordIndex) {
        String id2 = rs.getString("ID2");
        if (id2 != null) {
            Node childNode = new Node();
            childNode.setId(Integer.valueOf(id2));
            childNode.setName(rs.getString("NAME2"));
            childNode.setType(rs.getString("TYPE2"));
            childNode.setValue(rs.getDouble("VALUE2"));
            childNode.setParent(item);
            item.getChildren().add(childNode);
        }
    }

}
