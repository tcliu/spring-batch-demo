package app.service;

import app.exception.RetryableException;
import app.model.ApplicationLog;
import app.repository.ApplicationLogRepository;
import org.apache.commons.lang3.StringUtils;
import org.hibernate.exception.ConstraintViolationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.SQLException;

/**
 * Created by Liu on 10/15/2016.
 */
@Service
public class ApplicationLogService extends EntityService<ApplicationLog,Integer> {

    @Autowired
    private JsonService jsonService;

    public ApplicationLog construct(Object o, Throwable e) {
        ApplicationLog log = new ApplicationLog();
        log.setType("ERROR");
        log.setCode(getErrorCode(e));
        log.setException(getException(e));
        log.setMessage(getMessage(e));
        log.setContent(getContent(o));
        log.setRecoverable(isRecoverable(log.getMessage(), e) ? "Y" : "N");
        log.setStatus("OPEN");
        log.beforeSave();
        return log;
    }

    public ApplicationLogRepository getApplicationLogRepository() {
        return (ApplicationLogRepository) getJpaRepository();
    }

    private <T extends Exception> T findException(Throwable root, Class<T> target) {
        T e = null;
        for (Throwable t = root; e == null && t != null; t = t.getCause()) {
            if (target.isInstance(t)) {
                e = (T) t;
            }
        }
        return e;
    }

    private String getErrorCode(Throwable e) {
        SQLException sqle = findException(e, SQLException.class);
        return sqle == null ? null : sqle.getSQLState().toString();
    }

    private String getException(Throwable e) {
        SQLException sqle = findException(e, SQLException.class);
        Throwable t = sqle == null ? e : sqle;
        return t.getClass().getName();
    }

    private String getMessage(Throwable e) {
        SQLException sqle = findException(e, SQLException.class);
        return StringUtils.substring(sqle == null ? e.getMessage() : sqle.getMessage(), 0, 255);
    }

    private String getContent(Object obj) {
        return jsonService.toJson(obj);
    }

    private boolean isRecoverable(String message, Throwable e) {
        return findException(e, ConstraintViolationException.class) != null
            || findException(e, RetryableException.class) != null;
    }


}
