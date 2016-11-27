package app.batch.mapper;

import java.util.ArrayList;
import app.model.Account;
import app.model.AccountHolder;
import app.model.Position;
import org.springframework.jdbc.support.rowset.SqlRowSet;

/**
 * Created by Liu on 11/27/2016.
 */
public class AccountRowMapper implements StructuredItemRowMapper<Account> {

    @Override
    public boolean isNewItem(final Account item, final SqlRowSet rs) {
        return item.getId() != rs.getInt("ACCOUNT_ID");
    }

    @Override
    public Account newItem(final SqlRowSet rs) {
        Account account = new Account();
        account.setId(rs.getInt("ACCOUNT_ID"));
        account.setName(rs.getString("ACCOUNT_NAME"));
        account.setType(rs.getString("ACCOUNT_TYPE"));
        account.setAccountHolders(new ArrayList<>());
        account.setPositions(new ArrayList<>());
        return account;
    }

    @Override
    public void updateItem(final Account item, final SqlRowSet rs) {
        String accountHolderId = rs.getString("ACCOUNT_HOLDER_ID");
        if (accountHolderId != null) {
            AccountHolder accountHolder = new AccountHolder();
            accountHolder.setId(Integer.valueOf(accountHolderId));
            accountHolder.setName(rs.getString("ACCOUNT_HOLDER_NAME"));
            accountHolder.setAccount(item);
            item.getAccountHolders().add(accountHolder);
        }
        String positionId = rs.getString("POSITION_ID");
        if (positionId != null) {
            Position position = new Position();
            position.setId(Integer.valueOf(positionId));
            position.setInstrumentId(rs.getInt("INSTRUMENT_ID"));
            position.setQuantity(rs.getBigDecimal("QUANTITY"));
            position.setPrice(rs.getBigDecimal("PRICE"));
            position.setAccount(item);
            item.getPositions().add(position);
        }
    }
}
