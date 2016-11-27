package app.model;

import javax.persistence.*;
import javax.validation.constraints.NotNull;
import com.fasterxml.jackson.annotation.JsonBackReference;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

/**
 * Created by Liu on 11/27/2016.
 */
@Entity
@Table(name = "account_holder")
public class AccountHolder implements CrudEntity<Integer> {

    @NotNull
    @Id
    @Column(name = "id", columnDefinition = "INTEGER", nullable = false)
    private Integer id;

    @NotNull
    @Column(name = "name", length = 255, columnDefinition = "NVARCHAR2(255)", nullable = false)
    private String name;

    @JsonBackReference
    @ManyToOne
    @JoinColumn(name = "account_id", columnDefinition = "INTEGER",
            foreignKey = @ForeignKey(name = "account_holder_fk1"))
    private Account account;

    public AccountHolder() {

    }

    public AccountHolder(int id, String name) {
        this.id = id;
        this.name = name;
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

    public Account getAccount() {
        return account;
    }

    public void setAccount(final Account account) {
        this.account = account;
    }

    @Override
    public String toString() {
        return "AccountHolder{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", account=" + account +
                '}';
    }

    @Override
    public boolean equals(final Object o) {
        return EqualsBuilder.reflectionEquals(this, o,
                "id");
    }

    @Override
    public int hashCode() {
        return HashCodeBuilder.reflectionHashCode(this,
                "id");
    }

}
