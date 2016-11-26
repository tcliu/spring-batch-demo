package app.model;

import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.LastModifiedDate;
import org.springframework.data.jpa.domain.support.AuditingEntityListener;

import javax.persistence.*;
import java.time.LocalDateTime;

/**
 * Created by Liu on 10/15/2016.
 */
@Entity
@Table(name = "application_log")
@EntityListeners(AuditingEntityListener.class)
public class ApplicationLog implements CrudEntity<Integer> {

    @Id
    @Column(name = "id", columnDefinition = "INTEGER", nullable = false)
    @GeneratedValue
    private Integer id;

    @Column(name = "ref_id", columnDefinition = "INTEGER")
    private Integer referenceId;

    @Column(name = "type", length = 50, columnDefinition = "NVARCHAR(50)", nullable = false)
    private String type;

    @Column(name = "code", length = 10, columnDefinition = "VARCHAR(10)")
    private String code;

    @Column(name = "message", length = 255, columnDefinition = "NVARCHAR2(255)")
    private String message;

    @Column(name = "content", columnDefinition = "NCLOB")
    private String content;

    @Column(name = "exception", columnDefinition = "VARCHAR(100)")
    private String exception;

    @Column(name = "recoverable", length = 1, columnDefinition = "CHAR(1)")
    private String recoverable;

    @Version
    @Column(name = "update_count", columnDefinition = "INTEGER")
    private Integer updateCount;

    @Column(name = "status", length = 30, columnDefinition = "VARCHAR(30)", nullable = false)
    private String status;

    @CreatedDate
    @Column(name = "created", columnDefinition = "TIMESTAMP")
    private LocalDateTime created;

    @LastModifiedDate
    @Column(name = "last_updated", columnDefinition = "TIMESTAMP")
    private LocalDateTime lastUpdated;

    @PrePersist
    public void onPrePersist() {
        setCreated(LocalDateTime.now());
        setLastUpdated(LocalDateTime.now());
    }

    @PreUpdate
    public void onPreUpdate() {
        setLastUpdated(LocalDateTime.now());
    }

    @Override
    public Integer getId() {
        return id;
    }

    @Override
    public void setId(final Integer id) {
        this.id = id;
    }

    public Integer getReferenceId() {
        return referenceId;
    }

    public void setReferenceId(final Integer referenceId) {
        this.referenceId = referenceId;
    }

    public String getType() {
        return type;
    }

    public void setType(final String type) {
        this.type = type;
    }

    public String getCode() {
        return code;
    }

    public void setCode(final String code) {
        this.code = code;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(final String message) {
        this.message = message;
    }

    public String getContent() {
        return content;
    }

    public void setContent(final String content) {
        this.content = content;
    }

    public String getException() {
        return exception;
    }

    public void setException(final String exception) {
        this.exception = exception;
    }

    public String getRecoverable() {
        return recoverable;
    }

    public void setRecoverable(final String recoverable) {
        this.recoverable = recoverable;
    }

    public Integer getUpdateCount() {
        return updateCount;
    }

    public void setUpdateCount(final Integer updateCount) {
        this.updateCount = updateCount;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(final String status) {
        this.status = status;
    }

    public LocalDateTime getCreated() {
        return created;
    }

    public void setCreated(final LocalDateTime created) {
        this.created = created;
    }

    public LocalDateTime getLastUpdated() {
        return lastUpdated;
    }

    public void setLastUpdated(final LocalDateTime lastUpdated) {
        this.lastUpdated = lastUpdated;
    }
}
