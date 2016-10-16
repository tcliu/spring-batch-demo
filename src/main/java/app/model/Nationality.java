package app.model;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * Created by Liu on 10/15/2016.
 */
@Entity
@Table(name = "nationality")
public class Nationality implements CrudEntity<String> {

    @Id
    @Column(name = "code", length = 50, columnDefinition = "VARCHAR2(30)", nullable = false)
    private String code;

    @Column(name = "description", length = 255, columnDefinition = "VARCHAR2(255)")
    private String description;

    public String getCode() {
        return code;
    }

    public void setCode(final String code) {
        this.code = code;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(final String description) {
        this.description = description;
    }

    @Override
    public String getId() {
        return getCode();
    }

    @Override
    public void setId(final String s) {
        setCode(s);
    }
}
