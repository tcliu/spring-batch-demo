package app.batch.listener;

import app.model.ApplicationLog;
import app.model.BatchExecution;
import app.service.ApplicationLogService;
import app.service.BatchExecutionService;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.annotation.*;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by Liu on 10/15/2016.
 */
@Component
public class RerunListener implements StepExecutionListener {

    private final Logger logger = LoggerFactory.getLogger(RerunListener.class);

    @Autowired
    private ApplicationLogService applicationLogService;

    @Autowired
    private BatchExecutionService batchExecutionService;

    private Collection<Pair<Object,Throwable>> jobErrors;

    @BeforeJob
    public void beforeJob(JobExecution jobExecution) {
        logger.info("beforeJob {}", jobExecution);
        jobErrors = new ConcurrentLinkedQueue<>();
    }

    @AfterJob
    public void afterJob(JobExecution jobExecution) {
        logger.info("afterJob {}", jobExecution);

        Pair<Integer,Integer> readWriteCounts = jobExecution.getStepExecutions().stream()
                .map(se -> Pair.of(se.getReadCount() + se.getReadSkipCount(), se.getWriteCount()))
                .reduce(Pair.of(0, 0), (a,b) -> Pair.of(a.getLeft() + b.getLeft(), a.getRight() + b.getRight()));
        jobExecution.getExecutionContext().putInt("total", readWriteCounts.getLeft());
        jobExecution.getExecutionContext().putInt("success", readWriteCounts.getRight());
        boolean success = readWriteCounts.getLeft() == readWriteCounts.getRight();
        jobExecution.setExitStatus(success ? ExitStatus.COMPLETED : ExitStatus.FAILED);
        jobErrors.forEach(jobError -> jobExecution.addFailureException(jobError.getRight()));
        saveBatchExecution(jobExecution, jobErrors);

        logger.info("exceptions = {}", jobExecution.getAllFailureExceptions());

        jobErrors.clear();
        jobErrors = null;
    }

    @Transactional(rollbackFor = Throwable.class)
    private void saveBatchExecution(JobExecution jobExecution, Collection<Pair<Object,Throwable>> jobErrors) {
        BatchExecution batchExecution = getBatchExecution(jobExecution);
        // create batch execution
        updateForRerun(batchExecution, jobExecution);
        batchExecutionService.update(batchExecution).iterator().next();
    }

    private BatchExecution getBatchExecution(JobExecution jobExecution) {
        final String batchId = jobExecution.getJobParameters().getString("batchId");
        BatchExecution batchExecution = batchId == null ? null
                : batchExecutionService.get(Integer.valueOf(batchId));
        return batchExecution;
    }

    private void updateForRerun(BatchExecution batchExecution, final JobExecution jobExecution) {
        batchExecution.setSuccessCount(batchExecution.getSuccessCount() + jobExecution.getExecutionContext().getInt("success"));
        boolean success = batchExecution.getTotalCount() == batchExecution.getSuccessCount();
        batchExecution.setStatus(success ? "COMPLETED" : "FAILED");
    }

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        logger.info("beforeStep {}", stepExecution);
    }

    @AfterStep
    public ExitStatus afterStep(StepExecution stepExecution) {
        logger.info("afterStep {}", stepExecution);
        return new ExitStatus("custom");
    }

    @BeforeChunk
    public void beforeChunk(ChunkContext chunkContext) {
        logger.debug("beforeChunk {}", chunkContext);
    }

    @AfterChunk
    public void afterChunk(ChunkContext chunkContext) {
        logger.debug("afterChunk {}", chunkContext);
    }

    @AfterChunkError
    public void afterChunkError(ChunkContext chunkContext) {
        logger.debug("afterChunkError {}", chunkContext);
    }

    @BeforeRead
    public void beforeRead() {
        logger.debug("beforeRead");
    }

    @AfterRead
    public void afterRead(ApplicationLog item) {
        logger.debug("afterRead {}", item);
    }

    @OnSkipInRead
    public void onSkipInRead(Throwable t) {
        logger.info("onSkipInRead {}", t);
    }

    @OnReadError
    public void onReadError(Exception e) {
        logger.info("onReadError {}", e);
    }

    @BeforeWrite
    public void beforeWrite(List<? extends ApplicationLog> items) {
        logger.debug("beforeWrite {}", items);
    }

    @AfterWrite
    public void afterWrite(List<? extends ApplicationLog> items) {
        logger.debug("afterWrite {}", items);

        items.forEach(item -> {
            item.setStatus("CLOSED");
        });
        applicationLogService.update(items);
    }

    @OnSkipInWrite
    public void onSkipInWrite(ApplicationLog item, Throwable t) {
        logger.info("onSkipInWrite {} {}", item, t.getMessage());
        jobErrors.add(Pair.of(item, t));

        applicationLogService.update(item);
    }

    @OnWriteError
    public void onWriteError(Exception exception, List<? extends ApplicationLog> items) {
        logger.info("onWriteError {} {}", items, exception.getMessage());
    }
}
