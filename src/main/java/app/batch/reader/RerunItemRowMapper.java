package app.batch.reader;

import app.model.ApplicationLog;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by Liu on 10/17/2016.
 */
public class RerunItemRowMapper implements RowMapper<ApplicationLog> {

    @Override
    public ApplicationLog mapRow(final ResultSet rs, final int rowNum) throws SQLException {
        ApplicationLog log = new ApplicationLog();
        log.setId(rs.getInt("id"));
        log.setCode(rs.getString("code"));
        log.setReferenceId(rs.getInt("ref_id"));
        log.setType(rs.getString("type"));
        log.setRecoverable(rs.getString("recoverable"));
        log.setException(rs.getString("exception"));
        log.setMessage(rs.getString("message"));
        log.setStatus(rs.getString("status"));
        log.setContent(rs.getString("content"));
        log.setUpdateCount(rs.getInt("update_count"));
        // TODO: read created and last updated
        return log;
    }

}
