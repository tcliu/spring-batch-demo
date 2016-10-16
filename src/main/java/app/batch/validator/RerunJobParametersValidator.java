package app.batch.validator;

import app.model.BatchExecution;
import app.service.BatchExecutionService;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.JobParametersValidator;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Created by Liu on 10/16/2016.
 */
public class RerunJobParametersValidator implements JobParametersValidator {

    @Autowired
    private BatchExecutionService batchExecutionService;

    @Override
    public void validate(final JobParameters parameters) throws JobParametersInvalidException {
        final String batchId = parameters.getString("batchId");
        if (batchId == null) {
            throw new JobParametersInvalidException("Batch ID is not provided.");
        }
        BatchExecution batchExecution = batchExecutionService.get(Integer.valueOf(batchId));
        if (batchExecution == null) {
            throw new JobParametersInvalidException("Batch execution " + batchId + " does not exist.");
        }
        if ("COMPLETED".equals(batchExecution.getStatus())) {
            throw new JobParametersInvalidException("Batch execution " + batchId + " has been completed.");
        }
    }
}
