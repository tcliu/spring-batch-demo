package app.model;

import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.LastModifiedDate;

import javax.persistence.*;
import java.time.LocalDateTime;

/**
 * Created by Liu on 10/15/2016.
 */
@Entity
@Table(name = "batch_execution")
public class BatchExecution implements CrudEntity<Integer> {

    @Id
    @Column(name = "id", columnDefinition = "INTEGER", nullable = false)
    @GeneratedValue
    private Integer id;

    @Column(name = "job_name", length = 50, columnDefinition = "VARCHAR2(50)", nullable = false)
    private String jobName;

    @Column(name = "total_count", columnDefinition = "INTEGER", nullable = false)
    private Integer totalCount;

    @Column(name = "success_count", columnDefinition = "INTEGER", nullable = false)
    private Integer successCount;

    @Column(name = "status", length = 30, columnDefinition = "VARCHAR2(30)", nullable = false)
    private String status;

    @Column(name = "count", columnDefinition = "INTEGER", nullable = false)
    private Integer count;

    @CreatedDate
    @Column(name = "created", columnDefinition = "TIMESTAMP")
    private LocalDateTime created;

    @LastModifiedDate
    @Column(name = "last_updated", columnDefinition = "TIMESTAMP")
    private LocalDateTime lastUpdated;

    public void beforeSave() {
        if (getCreated() == null) {
            setCreated(LocalDateTime.now());
        }
        setLastUpdated(LocalDateTime.now());
        count = 1 + (count == null ? 0 : count);
    }

    @Override
    public Integer getId() {
        return id;
    }

    @Override
    public void setId(final Integer id) {
        this.id = id;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(final String jobName) {
        this.jobName = jobName;
    }

    public Integer getTotalCount() {
        return totalCount;
    }

    public void setTotalCount(final Integer totalCount) {
        this.totalCount = totalCount;
    }

    public Integer getSuccessCount() {
        return successCount;
    }

    public void setSuccessCount(final Integer successCount) {
        this.successCount = successCount;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(final String status) {
        this.status = status;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(final Integer count) {
        this.count = count;
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
