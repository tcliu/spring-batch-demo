package app.model;

import javax.persistence.*;
import javax.validation.constraints.NotNull;
import java.math.BigDecimal;
import com.fasterxml.jackson.annotation.JsonBackReference;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

/**
 * Created by Liu on 11/27/2016.
 */
@Entity
@Table(name = "position")
public class Position implements CrudEntity<Integer> {

    @NotNull
    @Id
    @Column(name = "id", columnDefinition = "INTEGER", nullable = false)
    private Integer id;

    @NotNull
    @Column(name = "instrument_id", columnDefinition = "INTEGER", nullable = false)
    private Integer instrumentId;

    @NotNull
    @Column(name = "quantity", precision = 15, scale = 2, columnDefinition = "NUMBER(15,2)", nullable = false)
    private BigDecimal quantity;

    @NotNull
    @Column(name = "price", precision = 20, scale = 6, columnDefinition = "NUMBER(20,6)", nullable = false)
    private BigDecimal price;

    @JsonBackReference
    @ManyToOne
    @JoinColumn(name = "account_id", columnDefinition = "INTEGER", nullable = false,
            foreignKey = @ForeignKey(name = "position_fk1"))
    private Account account;

    public Position() {

    }

    public Position(int id, int instrumentId, BigDecimal quantity, BigDecimal price) {
        this.id = id;
        this.instrumentId = instrumentId;
        this.quantity = quantity;
        this.price = price;
    }

    @Override
    public Integer getId() {
        return id;
    }

    @Override
    public void setId(final Integer id) {
        this.id = id;
    }

    public Integer getInstrumentId() {
        return instrumentId;
    }

    public void setInstrumentId(final Integer instrumentId) {
        this.instrumentId = instrumentId;
    }

    public BigDecimal getQuantity() {
        return quantity;
    }

    public void setQuantity(final BigDecimal quantity) {
        this.quantity = quantity;
    }

    public BigDecimal getPrice() {
        return price;
    }

    public void setPrice(final BigDecimal price) {
        this.price = price;
    }

    public Account getAccount() {
        return account;
    }

    public void setAccount(final Account account) {
        this.account = account;
    }

    @Override
    public String toString() {
        return "Position{" +
                "id=" + id +
                ", instrumentId=" + instrumentId +
                ", quantity=" + quantity +
                ", price=" + price +
                ", account=" + account +
                '}';
    }

    @Override
    public boolean equals(final Object o) {
        return EqualsBuilder.reflectionEquals(this, o, "id");
    }

    @Override
    public int hashCode() {
        return HashCodeBuilder.reflectionHashCode(this, "id");
    }
}
