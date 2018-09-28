package transaction;

import java.math.BigDecimal;
import java.util.Date;
import java.util.List;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import constant.Table;

public class OrderStatusTransaction {
    private Session session;

    private PreparedStatement selectCustomerStatement;
    private PreparedStatement selectOrderLineStatement;

    private static final String KEY_SPACE_WITH_DOT = Table.KEY_SPACE + ".";

    private static final String SELECT_CUSTOMER =
            " SELECT C_FIRST, C_MIDDLE, C_LAST, C_BALANCE, C_LAST_O_ID, C_O_ENTRY_D, C_O_CARRIER_ID FROM "
            + KEY_SPACE_WITH_DOT + Table.TABLE_CUSTOMER
            + " WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?; ";
    private static final String SELECT_ORDERLINE =
            " SELECT OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D FROM "
            + KEY_SPACE_WITH_DOT + Table.TABLE_ORDERLINE
            + " WHERE OL_W_ID = ? AND OL_D_ID = ? AND OL_O_ID = ?; ";

    public OrderStatusTransaction(Session session) {
        this.session = session;
        this.selectCustomerStatement = session.prepare(SELECT_CUSTOMER);
        this.selectOrderLineStatement = session.prepare(SELECT_ORDERLINE);
    }

    public void processOrderStatus(int wId, int dId, int cId) {
        Row customer = getCustomer(wId, dId, cId);

        String firstName = customer.getString("C_FIRST");
        String middleName = customer.getString("C_MIDDLE");
        String lastName = customer.getString("C_LAST");
        BigDecimal balance = customer.getDecimal("C_BALANCE");

        int oId = customer.getInt("C_LAST_O_ID");
        Date entryDate = customer.getTimestamp("C_O_ENTRY_D");
        int carrierId = customer.getInt("C_O_CARRIER_ID");

        // Print customer information
        System.out.println("Customer's name: " + firstName + " " + middleName + " " + lastName);
        System.out.println("Customer's balance: " + balance);

        // Print customer's last order
        System.out.println("Last Order ID: " + oId);
        System.out.println("Entry date and time: " + entryDate);
        System.out.println("Carrier identifier: " + carrierId);

        System.out.println("====================");

        List<Row> orderLines = getOrderLine(wId, dId, oId);
        for (Row orderLine : orderLines) {
            int iId = orderLine.getInt("OL_I_ID");
            int supplierWId = orderLine.getInt("OL_SUPPLY_W_ID");
            BigDecimal quantity = orderLine.getDecimal("OL_QUANTITY");
            BigDecimal amount = orderLine.getDecimal("OL_AMOUNT");
            Date deliveryData = orderLine.getTimestamp("OL_DELIVERY_D");

            System.out.println("Item number: " + iId);
            System.out.println("Supplying warehouse number: " + supplierWId);
            System.out.println("Quantity ordered " + quantity);
            System.out.println("Total price for ordered item: " + amount);
            System.out.println("Data and time of delivery: " + deliveryData);
            System.out.println();
        }
    }

    private Row getCustomer(int wId, int dId, int cId) {
        ResultSet rs = session.execute(selectCustomerStatement.bind(wId, dId, cId));
        List<Row> customers = rs.all();
        return (customers.isEmpty()) ? null : customers.get(0);
    }

    private List<Row> getOrderLine(int wId, int dId, int oId) {
        ResultSet rs = session.execute(selectOrderLineStatement.bind(wId, dId, oId));
        List<Row> orderLines = rs.all();
        return (orderLines.isEmpty()) ? null : orderLines;
    }
}
