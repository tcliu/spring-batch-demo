package app.batch.writer;

import app.model.ApplicationLog;
import app.service.ApplicationLogService;
import app.service.JsonService;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by Liu on 10/15/2016.
 */
public class RerunItemWriter<T> implements ItemWriter<ApplicationLog> {

    @Autowired
    private ApplicationLogService applicationLogService;

    @Autowired
    private JsonService jsonService;

    private final ItemWriter<T> itemWriter;

    private final Class<T> entityClass;

    public RerunItemWriter(ItemWriter<T> itemWriter, Class<T> entityClass) {
        this.itemWriter = itemWriter;
        this.entityClass = entityClass;
    }

    @Override
    public void write(final List<? extends ApplicationLog> items) throws Exception {
        List<T> entities = items.stream().map(item ->
            jsonService.fromJson(item.getContent(), entityClass)
        ).collect(Collectors.toList());
        itemWriter.write(entities);
    }
}
