package transaction;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.ResultSet;

import java.math.BigDecimal;
import java.util.Date;
import java.util.List;

import constant.Table;

public class DeliveryTransaction {
    private Session session;

    private PreparedStatement selectMinimumOrderStatement;
    private PreparedStatement selectOrderLinesStatement;
    private PreparedStatement selectCustomerStatement;
    private PreparedStatement updateOrdersStatement;
    private PreparedStatement updateOrderLineStatement;
    private PreparedStatement updateCustomerByDeliveryStatement;
    private  PreparedStatement updateCustomerOrderCarrierIdStatement;

    private Row selectedCustomer;
    private Row selectedMinimumOrder; // Oldest order not delivered

    private static final String KEY_SPACE_WITH_DOT = Table.KEY_SPACE + ".";

    private static final String SELECT_MINIMUM_ORDER =
            "SELECT O_ID, O_C_ID, O_ENTRY_D"
                    + " FROM " + KEY_SPACE_WITH_DOT + Table.TABLE_ORDER
                    + " WHERE O_W_ID = ? AND O_D_ID = ? AND O_CARRIER_ID = -1" // need to check the null value
                    + " ORDER BY O_ID ASC LIMIT 1 ALLOW FILTERING;";

    private static final String SELECT_ORDER_LINES =
            "SELECT OL_NUMBER, OL_AMOUNT "
                    + "FROM " + KEY_SPACE_WITH_DOT + Table.TABLE_ORDERLINE
                    + " WHERE OL_W_ID = ? AND OL_D_ID = ? AND OL_O_ID = ?;";

    private static final String SELECT_CUSTOMER =
            "SELECT C_BALANCE, C_DELIVERY_CNT, C_LAST_O_ID "
                    + "FROM " + KEY_SPACE_WITH_DOT + Table.TABLE_CUSTOMER
                    + " WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?;";

    private static final String UPDATE_CUSTOMER_BY_DELIVERY =
            "UPDATE " + KEY_SPACE_WITH_DOT + Table.TABLE_CUSTOMER
                    + " SET C_BALANCE = ?, C_DELIVERY_CNT = ?"
                    + " WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?;";

    private static final String UPDATE_ORDER_LINE =
            "UPDATE " + KEY_SPACE_WITH_DOT + Table.TABLE_ORDERLINE
                    + " SET OL_DELIVERY_D = ? "
                    + "WHERE OL_W_ID = ? AND OL_D_ID = ? AND OL_O_ID = ? AND OL_NUMBER = ?;";

    private static final String UPDATE_ORDERS =
            "UPDATE " + KEY_SPACE_WITH_DOT + Table.TABLE_ORDER
                    + " SET O_CARRIER_ID = ? "
                    + "WHERE O_W_ID = ? AND O_D_ID = ? AND O_ID = ? AND O_C_ID = ?;";

    private static final String UPDATE_CUSTOMER_O_C_ID =
            "UPDATE " + KEY_SPACE_WITH_DOT + Table.TABLE_CUSTOMER
                    + " SET C_O_CARRIER_ID = ? "
                    + "WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?;";

    public DeliveryTransaction(Session session) {
        this.session = session;
        this.selectMinimumOrderStatement = session.prepare(SELECT_MINIMUM_ORDER);
        this.selectOrderLinesStatement = session.prepare(SELECT_ORDER_LINES);
        this.selectCustomerStatement = session.prepare(SELECT_CUSTOMER);
        this.updateOrdersStatement = session.prepare(UPDATE_ORDERS);
        this.updateOrderLineStatement = session.prepare(UPDATE_ORDER_LINE);
        this.updateCustomerByDeliveryStatement = session.prepare(UPDATE_CUSTOMER_BY_DELIVERY);
        this.updateCustomerOrderCarrierIdStatement = session.prepare(UPDATE_CUSTOMER_O_C_ID);
    }

    public void processDeliveryTransaction(int W_ID, int CARRIER_ID) {
        for (int D_ID = 1; D_ID < 11; D_ID++) {
            selectMinimumOrder(W_ID, D_ID);

            int O_ID = selectedMinimumOrder.getInt("O_ID");
            int O_C_ID = selectedMinimumOrder.getInt("O_C_ID");
            updateOrderByCarrier(W_ID, D_ID, O_C_ID, O_ID, CARRIER_ID);

            // Get current date and time
            Date OL_DELIVERY_D = new Date();

            BigDecimal sumOfAllOrderLinesAmount = selectAndUpdateOrderLines(W_ID, D_ID, O_ID, OL_DELIVERY_D);
            selectCustomer(W_ID, D_ID, O_C_ID);
            updateCustomerByDelivery(W_ID, D_ID, O_C_ID, sumOfAllOrderLinesAmount, O_ID, CARRIER_ID);
        }
    }

    private void selectMinimumOrder(int O_W_ID, int O_D_ID) {
        ResultSet rs = session.execute(selectMinimumOrderStatement.bind(O_W_ID, O_D_ID));
        List<Row> orders = rs.all();
        if (!orders.isEmpty()) selectedMinimumOrder = orders.get(0);
    }

    private BigDecimal selectAndUpdateOrderLines(int W_ID, int D_ID, int O_ID, Date OL_DELIVERY_D) {
        ResultSet resultSet = session.execute(selectOrderLinesStatement.bind(W_ID, D_ID, O_ID));
        List<Row> resultRow = resultSet.all();
        BigDecimal finalSum = new BigDecimal(0);

        for (Row orderLine : resultRow) {
            int OL_NUMBER = orderLine.getInt("OL_NUMBER");

            BigDecimal currentOLAmount = orderLine.getDecimal("OL_AMOUNT");
            finalSum = finalSum.add(currentOLAmount);

            session.execute(updateOrderLineStatement.bind(OL_DELIVERY_D, W_ID, D_ID, O_ID, OL_NUMBER));
            System.out.println("Update Order Line: "
                   + OL_DELIVERY_D + " " + W_ID + " " + D_ID + " " + O_ID + " " + OL_NUMBER);
        }

        return finalSum;
    }

    private void selectCustomer(int C_W_ID, int C_D_ID, int C_ID) {
        ResultSet resultSet = session.execute(selectCustomerStatement.bind(C_W_ID, C_D_ID, C_ID));
        List<Row> customers = resultSet.all();
        if(!customers.isEmpty()) selectedCustomer = customers.get(0);
    }

    private void updateOrderByCarrier(int O_W_ID, int O_D_ID, int O_C_ID, int O_ID, int CARRIER_ID) {
        session.execute(updateOrdersStatement.bind(CARRIER_ID, O_W_ID, O_D_ID, O_ID, O_C_ID));
        System.out.println("Update Order By Carrier: "
                + O_W_ID + " " + O_D_ID + " " + O_C_ID + " " + O_ID + " " + CARRIER_ID);
    }

    private void updateCustomerByDelivery(int C_W_ID, int C_D_ID, int C_ID, BigDecimal OL_AMOUNT_SUM,
                                          int currentDeliveredO_ID, int givenCARRIER_ID) {
        // Increment C BALANCE by B, where B denote the sum of OL AMOUNT for all the items placed in order X
        BigDecimal C_BALANCE = selectedCustomer.getDecimal("C_BALANCE").add(OL_AMOUNT_SUM);

        // Increment C DELIVERY CNT by 1
        int C_DELIVERY_CNT = selectedCustomer.getInt("C_DELIVERY_CNT") + 1;
        session.execute(updateCustomerByDeliveryStatement.bind(C_BALANCE, C_DELIVERY_CNT, C_W_ID, C_D_ID, C_ID));
        System.out.println("Update Customer By Delivery: "
                + C_W_ID + " " + C_D_ID + " " + C_ID + " " + C_BALANCE + " " + C_DELIVERY_CNT);

        int C_LAST_O_ID = selectedCustomer.getInt("C_LAST_O_ID");
        if (C_LAST_O_ID == currentDeliveredO_ID) {
            session.execute(updateCustomerOrderCarrierIdStatement.bind(givenCARRIER_ID, C_W_ID, C_D_ID, C_ID));
            System.out.println("Update C_O_CARRIER_ID : " + C_LAST_O_ID + " " + givenCARRIER_ID);
        }
    }
}
