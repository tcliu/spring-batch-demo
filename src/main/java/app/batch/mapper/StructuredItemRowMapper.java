package app.batch.mapper;

import org.springframework.jdbc.support.rowset.SqlRowSet;

/**
 * Row mapper implementation for objects with nested structure such that each item can be
 * associated with one or multiple rows.
 */
public interface StructuredItemRowMapper<T> {

    /**
     * Determines if a new item should be initialized when iterating a row set.
     * @param item the item
     * @param rs the row set
     * @param recordIndex the record index (starts from 1)
     * @return {@code true} if a new item should be initialized
     */
    boolean isNewItem(T item, SqlRowSet rs, int recordIndex);

    /**
     * This method is invoked when {@code isNewItem(item, rs)} returns true. It should return a
     * new item based on the current row in the row set.
     * @param rs the row set
     * @param recordIndex the record index (starts from 1)
     * @return the new item
     */
    T newItem(SqlRowSet rs, int recordIndex);

    /**
     * Updates the item returned from {@code newItem(rs)} based on the current row in the row set.
     * It should include logic to associate the item with any sub-object data retrievable from the
     * row set.
     * @param item the current item
     * @param rs the row set
     * @param recordIndex the record index (starts from 1)
     */
    void updateItem(T item, SqlRowSet rs, int recordIndex);

}
