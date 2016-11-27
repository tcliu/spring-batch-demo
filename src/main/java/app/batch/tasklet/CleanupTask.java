package app.batch.tasklet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/**
 * Created by Liu on 10/16/2016.
 */
@Component
public class CleanupTask implements Tasklet {

    private final Logger logger = LoggerFactory.getLogger(CleanupTask.class);

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private String cleanupSql;

    @Override
    public RepeatStatus execute(final StepContribution contribution, final ChunkContext chunkContext) throws Exception {
        logger.info("Start cleanup ...");
        jdbcTemplate.execute(cleanupSql);
        logger.info("Cleanup done!");
        return RepeatStatus.FINISHED;
    }

    public String getCleanupSql() {
        return cleanupSql;
    }

    public void setCleanupSql(final String cleanupSql) {
        this.cleanupSql = cleanupSql;
    }
}
