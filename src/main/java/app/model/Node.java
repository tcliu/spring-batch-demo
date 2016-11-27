package app.model;

import javax.persistence.*;
import javax.validation.constraints.NotNull;
import java.util.List;
import com.fasterxml.jackson.annotation.JsonBackReference;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

/**
 * Created by Liu on 11/26/2016.
 */
@Entity
@Table(name = "node")
public class Node implements CrudEntity<Integer> {

    @NotNull
    @Id
    @Column(name = "id", columnDefinition = "INTEGER", nullable = false)
    private Integer id;

    @NotNull
    @Column(name = "name", length = 255, columnDefinition = "NVARCHAR2(255)", nullable = false)
    private String name;

    @NotNull
    @Column(name = "type", length = 20, columnDefinition = "VARCHAR2(20)", nullable = false)
    private String type;

    @Column(name = "value", length = 255, columnDefinition = "NUMBER(38,16)")
    private double value;

    @JsonBackReference
    @ManyToOne
    @JoinColumn(name = "parent_id", columnDefinition = "INTEGER", foreignKey = @ForeignKey(name = "node_fk1"))
    private Node parent;

    @OneToMany(fetch = FetchType.EAGER, cascade={CascadeType.REFRESH, CascadeType.MERGE, CascadeType.PERSIST}, mappedBy = "parent")
    private List<Node> children;

    public Node() {

    }

    public Node(Integer id, String name, String type, double value) {
        this.id = id;
        this.name = name;
        this.type = type;
        this.value = value;
    }

    @Override
    public boolean equals(final Object o) {
        return EqualsBuilder.reflectionEquals(this, o,
                "id", "parent", "children");
    }

    @Override
    public int hashCode() {
        return HashCodeBuilder.reflectionHashCode(this,
                "id", "parent", "children");
    }

    @Override
    public String toString() {
        return "Node{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", type='" + type + '\'' +
                ", value=" + value +
                ", parent=" + parent +
                '}';
    }

    @Override
    public Integer getId() {
        return id;
    }

    @Override
    public void setId(final Integer id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(final String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(final String type) {
        this.type = type;
    }

    public double getValue() {
        return value;
    }

    public void setValue(final double value) {
        this.value = value;
    }

    public Node getParent() {
        return parent;
    }

    public void setParent(final Node parent) {
        this.parent = parent;
    }

    public List<Node> getChildren() {
        return children;
    }

    public void setChildren(final List<Node> children) {
        this.children = children;
        children.forEach(c -> c.setParent(this));
    }

}
