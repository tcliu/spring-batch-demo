package app;

import java.util.Arrays;
import java.util.Date;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.*;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

@SpringBootApplication(scanBasePackages = "app")
public class BatchApplication {

    private static Logger logger = LoggerFactory.getLogger(BatchApplication.class);

    public static void main(String[] args) throws Exception {
        final SpringApplication app = new SpringApplication();
        app.setWebEnvironment(false);
        final Object[] sources = {
            BatchApplication.class, "classpath:/batch-jobs.xml",
        };
        int exitCode = 0;
        final JobParameters jobParameters = getJobParameters(args);
        try (final ConfigurableApplicationContext ctx = app.run(sources, args)) {
            JobLauncher jobLauncher = ctx.getBean(JobLauncher.class);
            String jobId = getJobId(jobParameters);
            Job job = ctx.getBean(jobId, Job.class);
            JobExecution jobExecution = jobLauncher.run(job, jobParameters);
            logger.info("Status = {}", jobExecution.getStatus());
            logger.info("Exit status = {}", jobExecution.getExitStatus());
            logger.info("Execution context = {}", jobExecution.getExecutionContext());
            exitCode = getExitCode(jobExecution.getExitStatus());
        } catch (Exception e) {
            logger.error("Encountered exception.", e);
            exitCode = -1;
        }
        System.exit(exitCode);
    }

    private static String getJobId(JobParameters jobParameters) {
        String jobId = jobParameters.getString("jobId");
        if (jobId != null) {
            return jobId;
        }
        return "Y".equalsIgnoreCase(jobParameters.getString("rerun")) ? "rerunJob" : "mainJob";
    }

    private static JobParameters getJobParameters(String[] args) {
        JobParametersBuilder jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addDate("date", new Date());
        Arrays.asList(args).stream().forEach(arg -> {
            int idx = arg.indexOf("=");
            if (idx > 0 && idx < arg.length() - 1) {
                String key = arg.substring(0, idx);
                String value = arg.substring(idx + 1, arg.length());
                jobParametersBuilder.addString(key, value);
            }
        });
        return jobParametersBuilder.toJobParameters();
    }

    private static int getExitCode(ExitStatus exitStatus) {
        int index = Arrays.asList(ExitStatus.UNKNOWN, ExitStatus.STOPPED,
                ExitStatus.NOOP, ExitStatus.EXECUTING, ExitStatus.FAILED).indexOf(exitStatus);
        int exitCode = index == -1 ? 0 : -index - 1;
        return exitCode;
    }
}