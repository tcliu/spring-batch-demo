package app.batch.listener;

import app.model.ApplicationLog;
import app.model.BatchExecution;
import app.service.ApplicationLogService;
import app.service.BatchExecutionService;
import app.service.PersonService;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.*;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

/**
 * Created by Liu on 10/14/2016.
 */
@Component
public class BatchListener {

    private Logger logger = LoggerFactory.getLogger(BatchListener.class);

    @Autowired
    private PersonService personService;

    @Autowired
    private ApplicationLogService applicationLogService;

    @Autowired
    private BatchExecutionService batchExecutionService;

    private Collection<Pair<Object,Throwable>> jobErrors;

    @BeforeJob
    public void beforeJob(JobExecution jobExecution) {
        logger.info("beforeJob {}", jobExecution);
        jobErrors = new ConcurrentLinkedQueue<>();
        resetApplicationLogs(jobExecution);
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
        createBatchExecution(jobExecution, jobErrors);

        logger.info("exceptions = {}", jobExecution.getAllFailureExceptions());
        logger.info("Persons = {}", personService.getJpaRepository().count());
        logger.info("Batch executions = {}", batchExecutionService.getJpaRepository().count());
        logger.info("Application logs = {}", applicationLogService.getJpaRepository().count());

        jobErrors.clear();
        jobErrors = null;
    }

    @Transactional(rollbackFor = Throwable.class)
    private void resetApplicationLogs(JobExecution jobExecution) {
        BatchExecution batchExecution = getBatchExecution(jobExecution);
        if (batchExecution != null) {
            List<ApplicationLog> appLogs = applicationLogService.getApplicationLogRepository().findByReferenceIdAndStatus(batchExecution.getId(), "OPEN");
            appLogs.forEach(appLog -> {
                appLog.setStatus("CANCELLED");
                appLog.beforeSave();
            });
            applicationLogService.update(appLogs);
        }
    }

    @Transactional(rollbackFor = Throwable.class)
    private void createBatchExecution(JobExecution jobExecution, Collection<Pair<Object,Throwable>> jobErrors) {
        BatchExecution batchExecution = getBatchExecution(jobExecution);
        if (batchExecution == null) {
            batchExecution = new BatchExecution();
            // create batch execution
            update(batchExecution, jobExecution);
            batchExecutionService.create(batchExecution).iterator().next();
        } else {
            // update batch execution
            update(batchExecution, jobExecution);
            batchExecutionService.save(batchExecution).iterator().next();
        }
        final BatchExecution be = batchExecution;
        final List<ApplicationLog> applicationLogs = jobErrors.stream().map(jobError -> {
            ApplicationLog log = applicationLogService.construct(jobError.getLeft(), jobError.getRight());
            log.setReferenceId(be.getId());
            return log;
        }).collect(Collectors.toList());
        applicationLogService.create(applicationLogs);
    }

    private BatchExecution getBatchExecution(JobExecution jobExecution) {
        final String batchId = jobExecution.getJobParameters().getString("batchId");
        BatchExecution batchExecution = batchId == null ? null
            : batchExecutionService.get(Integer.valueOf(batchId));
        return batchExecution;
    }

    private void update(BatchExecution batchExecution, final JobExecution jobExecution) {
        batchExecution.setJobName(jobExecution.getJobInstance().getJobName());
        batchExecution.setTotalCount(jobExecution.getExecutionContext().getInt("total"));
        batchExecution.setSuccessCount(jobExecution.getExecutionContext().getInt("success"));
        boolean success = batchExecution.getTotalCount() == batchExecution.getSuccessCount();
        batchExecution.setStatus(success ? "COMPLETED" : "FAILED");
        batchExecution.beforeSave();
    }

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        logger.info("beforeStep {}", stepExecution);
    }

    @AfterStep
    public void afterStep(StepExecution stepExecution) {
        logger.info("afterStep {}", stepExecution);
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
    public void afterRead(Object item) {
        logger.debug("afterRead {}", item);
    }

    @OnSkipInRead
    public void onSkipInRead(Throwable t) {
        logger.info("onSkipInRead {}", t.getMessage());
    }

    @OnReadError
    public void onReadError(Exception e) {
        logger.info("onReadError {}", e.getMessage());
    }

    @BeforeProcess
    public void beforeProcess(Object item) {
        logger.debug("beforeProcess {}", item);
    }

    @AfterProcess
    public void afterProcess(Object item, Object result) {
        logger.debug("afterProcess {} {}", item, result);
    }

    @OnSkipInProcess
    public void onSkipInProcess(Object item, Throwable t) {
        logger.info("onSkipInProcess {} {}", item, t.getMessage());
        jobErrors.add(Pair.of(item, t));
    }

    @OnProcessError
    public void onProcessError(Object item, Exception e) {
        logger.info("onProcessError {} {}", item, e.getMessage());
    }

    @BeforeWrite
    public void beforeWrite(List<?> items) {
        logger.debug("beforeWrite {}", items);
    }

    @AfterWrite
    public void afterWrite(List<?> items) {
        logger.debug("afterWrite {}", items);
    }

    @OnSkipInWrite
    public void onSkipInWrite(Object item, Throwable t) {
        logger.info("onSkipInWrite {} {}", item, t.getMessage());
        jobErrors.add(Pair.of(item, t));
    }

    @OnWriteError
    public void onWriteError(Exception exception, List<?> items) {
        logger.info("onWriteError {}", items);
    }

}
