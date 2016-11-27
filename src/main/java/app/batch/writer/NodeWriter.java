package app.batch.writer;

import java.util.List;
import app.model.Node;
import org.springframework.batch.item.ItemWriter;

/**
 * Created by Liu on 11/27/2016.
 */
public class NodeWriter implements ItemWriter<Node> {

    @Override
    public void write(final List<? extends Node> items) throws Exception {
        System.out.println(items);
    }
}
