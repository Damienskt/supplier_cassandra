package transaction;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import java.util.List;
import java.util.ArrayList;

import constant.Table;

public class TopBalanceTransaction {

    private PreparedStatement customerNameCql;
    private String KEY_SPACE_WITH_DOT;
    private Session session;
    private PreparedStatement topBalanceCql;

    private String CUSTOMER_NAME_QUERY =
            "SELECT c_first, c_middle, c_last "
                    + "FROM "+ KEY_SPACE_WITH_DOT +"customer "
                    + "WHERE c_w_id=? AND c_d_id=? AND c_id = ?;";
    private String TOP_BALANCE_QUERY =
            "SELECT * "
                    + "FROM "+ KEY_SPACE_WITH_DOT +"customer_balances "
                    + "LIMIT 10;";

    public TopBalanceTransaction(Session session, String keySpace) {
        this.KEY_SPACE_WITH_DOT = keySpace + ".";

        this.CUSTOMER_NAME_QUERY =
                "SELECT c_first, c_middle, c_last "
                        + "FROM "+ KEY_SPACE_WITH_DOT +"customer "
                        + "WHERE c_w_id=? AND c_d_id=? AND c_id = ?;";
        this.TOP_BALANCE_QUERY =
                "SELECT * "
                        + "FROM "+ KEY_SPACE_WITH_DOT +"customer_balances "
                        + "LIMIT 10;";

        this.session = session;
        topBalanceCql = session.prepare(TOP_BALANCE_QUERY);
        customerNameCql = session.prepare(CUSTOMER_NAME_QUERY);
    }

    /* Start of public methods */
    public void calTopBalance() {
        ResultSet resultSet = session.execute(topBalanceCql.bind()); //get the top 10 customer with highest balance
        List<Row> customerNames = new ArrayList(); 
        List<Row> topCustomers = resultSet.all();
        for(Row cus: topCustomers){
            ResultSet customerName = session.execute(customerNameCql.bind(cus.getInt("c_w_id"),
                    cus.getInt("c_d_id"), cus.getInt("c_id"))); //retrieve each customer name for output
            Row cusN = (customerName.all()).get(0);
            customerNames.add(cusN);
        }
        printTopBalance(topCustomers, customerNames);
    }

    private void printTopBalance(List<Row> topCustomers, List<Row> customerNames){
        for (int i=0; i<=9; i++){
            Row cusName = customerNames.get(i);
            System.out.println("customer name: " + cusName.getString("c_first") + " "
                    + cusName.getString("c_middle") + " " + cusName.getString("c_last"));
            Row cus = topCustomers.get(i);
            System.out.println("customer balance: " + (cus.getDecimal("c_balance")).intValue());
            System.out.println("W_Id: " + cus.getInt("c_w_id") + " D_Id: " + cus.getInt("c_d_id"));
        }
    }
}
