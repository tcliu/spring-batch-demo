package app.repository;

import javax.annotation.Resource;
import java.util.Arrays;
import app.BatchApplication;
import app.model.Node;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;

/**
 * Created by Liu on 11/26/2016.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = BatchApplication.class)
public class NodeRepositoryTest {

    private static Logger LOGGER = LoggerFactory.getLogger(NodeRepositoryTest.class);

    @Resource
    private NodeRepository nodeRepository;

    @Resource
    private JdbcTemplate jdbcTemplate;

    @Before
    public void setup() {
        Node root = new Node(1, "Root", "P", 1.0);
        Node branch = new Node(2, "Branch", "C", 1.0);
        root.setChildren(Arrays.asList(branch));
        nodeRepository.save(root);
    }

    @After
    public void tearDown() {
        nodeRepository.deleteAll();
    }

    @Test
    public void count() {
        assertThat(nodeRepository.count(), equalTo(2L));
    }

    @Test
    public void create() {
        Node hello = new Node(2, "Hello", "P", 1.0);
        nodeRepository.save(hello);
        Node hello2 = nodeRepository.findOne(2);
        assertThat(hello2, equalTo(hello));
    }

    @Test
    public void createWithParent() {
        Node parent = new Node(5, "Parent", "P", 2.0);
        nodeRepository.save(parent);
        Node hello = new Node(2, "Hello", "C", 1.0);
        hello.setParent(parent);
        nodeRepository.save(hello);
        Node hello2 = nodeRepository.findOne(2);
        assertThat(hello2, equalTo(hello));
        assertThat(hello2.getParent(), equalTo(parent));
    }

    @Test
    public void createWithChildren() {
        Node hello = new Node(2, "Hello", "P", 1.0);
        Node child = new Node(3, "Child", "C", 1.1);
        Node child2 = new Node(4, "Child2", "C", 1.2);
        hello.setChildren(Arrays.asList(child, child2));

        nodeRepository.save(hello);

        Node root2 = nodeRepository.findOne(2);
        assertThat(root2.getChildren().get(0), equalTo(child));
        assertThat(root2.getChildren().get(1), equalTo(child2));
        assertThat(root2.getChildren().get(0).getParent(), equalTo(hello));
        assertThat(root2.getChildren().get(1).getParent(), equalTo(hello));
    }

}
