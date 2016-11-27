package app.batch.reader;

import javax.sql.DataSource;
import java.sql.SQLException;
import app.batch.mapper.StructuredItemRowMapper;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;

/**
 * Item reader implementation for objects with nested structure.
 */
public class StructuredItemReader<T> extends AbstractItemCountingItemStreamItemReader<T>
        implements InitializingBean, BeanNameAware {

    private StructuredItemRowMapper<T> rowMapper;

    private DataSource dataSource;

    private JdbcTemplate jdbcTemplate;

    private SqlRowSet sqlRowSet;

    private String sql;

    private Object[] parameters;

    private boolean hasNext;

    private boolean doRead;

    private T curItem;

    @Override
    public void setBeanName(final String name) {
        setName(name);
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        if (dataSource == null) {
            throw new IllegalStateException("Data source is not set.");
        }
        if (sql == null) {
            throw new IllegalStateException("SQL is not defined.");
        }
        if (rowMapper == null) {
            throw new IllegalStateException("Row mapper is not defined.");
        }
        if (jdbcTemplate == null) {
            jdbcTemplate = new JdbcTemplate(dataSource);
        }
    }

    @Override
    protected T doRead() throws Exception {
        final int recordIndex = getCurrentItemCount();
        while (doRead && hasNext) {
            if (curItem != null && rowMapper.isNewItem(curItem, sqlRowSet, recordIndex)) {
                doRead = false;
            } else {
                if (curItem == null) {
                    curItem = rowMapper.newItem(sqlRowSet, recordIndex);
                }
                rowMapper.updateItem(curItem, sqlRowSet, recordIndex);
                hasNext = sqlRowSet.next();
            }
        }
        T item = curItem;
        if (curItem != null) {
            curItem = null;
            doRead = hasNext;
        }
        return item;
    }

    @Override
    protected void doOpen() throws SQLException {
        sqlRowSet = parameters == null ?
            jdbcTemplate.queryForRowSet(sql) :
            jdbcTemplate.queryForRowSet(sql, parameters);
        doRead = hasNext = sqlRowSet.next();
    }

    @Override
    protected void doClose() throws Exception {
        sqlRowSet = null;
    }

    public StructuredItemRowMapper<T> getRowMapper() {
        return rowMapper;
    }

    public void setRowMapper(final StructuredItemRowMapper<T> rowMapper) {
        this.rowMapper = rowMapper;
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    public void setDataSource(final DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(final String sql) {
        this.sql = sql;
    }

    public Object[] getParameters() {
        return parameters;
    }

    public void setParameters(final Object[] parameters) {
        this.parameters = parameters;
    }

}
