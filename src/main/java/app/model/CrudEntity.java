package app.model;

/**
 * Created by Liu on 10/15/2016.
 */
public interface CrudEntity<ID> {

    ID getId();

    void setId(ID id);
}
