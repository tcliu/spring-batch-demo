package app.batch.reader;

import javax.annotation.Resource;
import javax.sql.DataSource;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import app.model.Account;
import app.model.AccountHolder;
import app.model.Position;
import app.repository.AccountHolderRepository;
import app.repository.AccountRepository;
import app.repository.NodeRepositoryTest;
import app.repository.PositionRepository;
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
import org.springframework.transaction.annotation.Transactional;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Created by Liu on 11/27/2016.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:/batch/account-test-context.xml"})
public class AccountReaderTest {

    private static Logger LOGGER = LoggerFactory.getLogger(NodeRepositoryTest.class);

    @Resource
    private AccountRepository accountRepository;

    @Resource
    private AccountHolderRepository accountHolderRepository;

    @Resource
    private PositionRepository positionRepository;

    @Resource
    private DataSource dataSource;

    @Resource
    private JdbcTemplate jdbcTemplate;

    @Resource
    private StructuredItemReader<Account> accountReader;

    private List<Account> accounts;

    private List<AccountHolder> accountHolders;

    private List<Position> positions;

    @Before
    public void setup() {
        if (accounts == null) {
            accounts = Arrays.asList(
                new Account(1, "Stock", "S"),
                new Account(2, "Cash", "C")
            );
            accountHolders = Arrays.asList(
                new AccountHolder(1, "Holder1"),
                new AccountHolder(2, "Holder2"),
                new AccountHolder(3, "Holder3")
            );
            positions = Arrays.asList(
                new Position(1, 5, new BigDecimal("400.00"), new BigDecimal("63.600000")),
                new Position(2, 12, new BigDecimal("1000.00"), new BigDecimal("14.500000"))
            );
            accounts.get(0).setAccountHolders(Arrays.asList(accountHolders.get(0), accountHolders.get(1)));
            accounts.get(0).setPositions(Arrays.asList(positions.get(0)));
            accounts.get(1).setAccountHolders(Arrays.asList(accountHolders.get(2)));
            accounts.get(1).setPositions(Arrays.asList(positions.get(1)));
        }
        accounts.forEach(account -> accountRepository.saveAndFlush(account));
    }

    @After
    public void tearDown() {
        accountRepository.deleteAll();
    }

    @Transactional
    @Test
    public void verify() {
        assertThat(accountRepository.count(), equalTo(2L));
        assertThat(accountHolderRepository.count(), equalTo(3L));
        assertThat(positionRepository.count(), equalTo(2L));
        Account acc1 = accountRepository.findOne(1);
        assertThat(acc1.getAccountHolders().size(), equalTo(2));
        assertThat(acc1.getPositions().size(), equalTo(1));
        Account acc2 = accountRepository.findOne(2);
        assertThat(acc2.getAccountHolders().size(), equalTo(1));
        assertThat(acc2.getPositions().size(), equalTo(1));
    }

    @Transactional
    @Test
    public void read() throws Exception {
        ExecutionContext executionContext = mock(ExecutionContext.class);
        accountReader.open(executionContext);

        Account acc1 = accountReader.read();
        assertThat(acc1, equalTo(accounts.get(0)));
        assertThat(acc1.getAccountHolders().get(0), equalTo(accountHolders.get(0)));
        assertThat(acc1.getAccountHolders().get(1), equalTo(accountHolders.get(1)));
        assertThat(acc1.getPositions().get(0), equalTo(positions.get(0)));

        Account acc2 = accountReader.read();
        assertThat(acc2, equalTo(accounts.get(1)));
        assertThat(acc2.getAccountHolders().get(0), equalTo(accountHolders.get(2)));
        assertThat(acc2.getPositions().get(0), equalTo(positions.get(1)));
        assertThat(accountReader.read(), nullValue());

        accountReader.close();
    }


}
