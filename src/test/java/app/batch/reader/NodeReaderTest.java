package app.batch.reader;

import javax.annotation.Resource;
import javax.sql.DataSource;
import java.util.Arrays;
import java.util.List;
import app.model.Node;
import app.repository.NodeRepository;
import app.repository.NodeRepositoryTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Created by Liu on 11/27/2016.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:/batch/node-test-context.xml"})
public class NodeReaderTest {

    private static Logger LOGGER = LoggerFactory.getLogger(NodeRepositoryTest.class);

    @Resource
    private NodeRepository nodeRepository;

    @Resource
    private DataSource dataSource;

    @Resource
    private JdbcTemplate jdbcTemplate;

    @Resource
    private StructuredItemReader<Node> nodeReader;

    private List<Node> nodes;

    @Before
    public void setup() {
        if (nodes == null) {
            nodes = Arrays.asList(
                new Node(1, "Parent1", "P", 1.0),
                new Node(2, "Parent2", "P", 1.0),
                new Node(3, "Child1", "C1", 1.0),
                new Node(4, "Child2", "C2", 1.0),
                new Node(5, "Child3", "C1", 1.0)
            );
            nodes.get(2).setParent(nodes.get(0));
            nodes.get(3).setParent(nodes.get(0));
            nodes.get(4).setParent(nodes.get(1));
        }
        nodes.forEach(node -> nodeRepository.saveAndFlush(node));
    }

    @After
    public void tearDown() {
        nodeRepository.deleteAll();
    }

    @Test
    public void verify() {
        assertThat(nodeRepository.count(), equalTo(5L));
        Node parent1 = nodeRepository.findOne(1);
        Node child1 = nodeRepository.findOne(3);
        assertThat(parent1.getChildren().size(), equalTo(2));
        assertThat(child1.getParent(), equalTo(parent1));
    }

    @Test
    public void read() throws Exception {
        ExecutionContext executionContext = mock(ExecutionContext.class);
        nodeReader.open(executionContext);

        Node parent1 = nodeReader.read();
        assertThat(parent1, equalTo(nodes.get(0)));
        assertThat(parent1.getChildren().get(0), equalTo(nodes.get(2)));
        assertThat(parent1.getChildren().get(1), equalTo(nodes.get(3)));
        Node parent2 = nodeReader.read();
        assertThat(parent2, equalTo(nodes.get(1)));
        assertThat(parent2.getChildren().get(0), equalTo(nodes.get(4)));
        assertThat(nodeReader.read(), nullValue());

        nodeReader.close();
    }


}
