package app.model;

import javax.persistence.*;
import javax.validation.constraints.NotNull;
import java.util.List;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

/**
 * Created by Liu on 11/27/2016.
 */
@Entity
@Table(name = "account")
public class Account implements CrudEntity<Integer> {

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

    @OneToMany(cascade = CascadeType.ALL, mappedBy = "account")
    private List<AccountHolder> accountHolders;

    @OneToMany(cascade = CascadeType.ALL, mappedBy = "account")
    private List<Position> positions;

    public Account() {

    }

    public Account(final Integer id, final String name, final String type) {
        this.id = id;
        this.name = name;
        this.type = type;
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

    public List<Position> getPositions() {
        return positions;
    }

    public void setPositions(final List<Position> positions) {
        this.positions = positions;
        positions.forEach(pos -> pos.setAccount(this));
    }

    public List<AccountHolder> getAccountHolders() {
        return accountHolders;
    }

    public void setAccountHolders(final List<AccountHolder> accountHolders) {
        this.accountHolders = accountHolders;
        accountHolders.forEach(ah -> ah.setAccount(this));
    }

    @Override
    public String toString() {
        return "Account{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", type='" + type + '\'' +
                '}';
    }

    @Override
    public boolean equals(final Object o) {
        return EqualsBuilder.reflectionEquals(this, o,
                "id", "accountHolders", "positions");
    }

    @Override
    public int hashCode() {
        return HashCodeBuilder.reflectionHashCode(this,
                "id", "accountHolders", "positions");
    }
}
