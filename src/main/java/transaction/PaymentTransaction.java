package transaction;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.ResultSet;

import java.util.List;
import java.math.BigDecimal;

import constant.Table;

public class PaymentTransaction {
    private Session session;

    private PreparedStatement selectWarehouseStatement;
    private PreparedStatement selectDistrictStatement;
    private PreparedStatement selectCustomerStatement;
    private PreparedStatement updateWarehouseYTDStatement;
    private PreparedStatement updateDistrictYTDStatement;
    private PreparedStatement updateCustomerByPaymentStatement;

    private static final String KEY_SPACE_WITH_DOT = Table.KEY_SPACE + ".";

    private static final String SELECT_WAREHOUSE =
            "SELECT W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP, W_YTD"
            + " FROM " +  KEY_SPACE_WITH_DOT + Table.TABLE_WAREHOUSE
            + " WHERE W_ID = ?;";

    private static final String SELECT_DISTRICT =
            "SELECT D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, D_YTD"
            + " FROM " + KEY_SPACE_WITH_DOT + Table.TABLE_DISTRICT
            + " WHERE D_W_ID = ? AND D_ID = ?;";

    private static final String SELECT_CUSTOMER =
            "SELECT C_W_ID, C_D_ID, C_ID, " +
                    "C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, " +
                    "C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, " +
                    "C_YTD_PAYMENT, C_PAYMENT_CNT"
            + " FROM " + KEY_SPACE_WITH_DOT + Table.TABLE_CUSTOMER
            + " WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?;";

    private static final String UPDATE_WAREHOUSE_YTD =
            "UPDATE " + KEY_SPACE_WITH_DOT + Table.TABLE_WAREHOUSE
            + " SET W_YTD = ?"
            + " WHERE W_ID = ?;";

    private static final String UPDATE_DISTRICT_YTD =
            "UPDATE " + KEY_SPACE_WITH_DOT + Table.TABLE_DISTRICT
                    + " SET D_YTD = ?"
                    + " WHERE D_W_ID = ? AND D_ID = ?;";

    private static final String UPDATE_CUSTOMER_BY_PAYMENT =
            "UPDATE " + KEY_SPACE_WITH_DOT + Table.TABLE_CUSTOMER
                    + " SET C_BALANCE = ?, C_YTD_PAYMENT = ?, C_PAYMENT_CNT = ? "
                    + " WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?;";

    private static final String MESSAGE_WAREHOUSE =
            "Warehouse address: Street(%1$s %2$s) City(%3$s) State(%4$s) Zip(%5$s)";
    private static final String MESSAGE_DISTRICT =
            "District address: Street(%1$s %2$s) City(%3$s) State(%4$s) Zip(%5$s)";
    private static final String MESSAGE_CUSTOMER =
            "Customer: Identifier(%1$s, %2$s, %3$s), "
            + "Name(%4$s, %5$s, %6$s), "
            + "Address(%7$s, %8$s, %9$s, %10$s, %11$s), "
            + "Phone(%12$s), Since(%13$s), "
            + "Credits(%14$s, %15$s, %16$s, %17$s)";
    private static final String MESSAGE_PAYMENT = "Payment amount: %1$s";

    public PaymentTransaction(Session session) {
        this.session = session;
        this.selectWarehouseStatement = session.prepare(SELECT_WAREHOUSE);
        this.selectDistrictStatement = session.prepare(SELECT_DISTRICT);
        this.selectCustomerStatement = session.prepare(SELECT_CUSTOMER);
        this.updateWarehouseYTDStatement = session.prepare(UPDATE_WAREHOUSE_YTD);
        this.updateDistrictYTDStatement = session.prepare(UPDATE_DISTRICT_YTD);
        this.updateCustomerByPaymentStatement = session.prepare(UPDATE_CUSTOMER_BY_PAYMENT);
    }

    public void processPaymentTransaction(int W_ID, int D_ID, int C_ID, float paymentAmount) {
        Row warehouseRelation = getWarehouseRelation(W_ID);
        Row districtRelation = getDistrictRelation(W_ID, D_ID);
        Row customerRelation = getCustomerRelation(W_ID, D_ID, C_ID);

        BigDecimal paymentDecimal = new BigDecimal(paymentAmount);

        updateWarehouse(W_ID, paymentDecimal, warehouseRelation);
        updateDistrict(W_ID, D_ID, paymentDecimal, districtRelation);
        updateCustomer(W_ID, D_ID, C_ID, paymentDecimal, customerRelation);

        outputPaymentTransactionResults(warehouseRelation, districtRelation, customerRelation,
                paymentAmount);
    }

    private Row getWarehouseRelation(int W_ID) {
        ResultSet rs = session.execute(selectWarehouseStatement.bind(W_ID));
        List<Row> warehouses = rs.all();
        return (warehouses.isEmpty()) ? null : warehouses.get(0);
    }

    private Row getDistrictRelation(int D_W_ID, int D_ID) {
        ResultSet rs = session.execute(selectDistrictStatement.bind(D_W_ID, D_ID));
        List<Row> districts = rs.all();
        return (districts.isEmpty()) ? null : districts.get(0);
    }

    private Row getCustomerRelation(int C_W_ID, int C_D_ID, int C_ID) {
        ResultSet rs = session.execute(selectCustomerStatement.bind(C_W_ID, C_D_ID, C_ID));
        List<Row> customers = rs.all();
        return (customers.isEmpty()) ? null : customers.get(0);
    }

    /* Update methods */
    private void updateWarehouse(int W_ID, BigDecimal paymentAmount, Row warehouseRelation) {
        //Update the warehouse C W ID by incrementing W YTD by PAYMENT
        BigDecimal W_YTD = warehouseRelation.getDecimal("W_YTD").add(paymentAmount);
        session.execute(updateWarehouseYTDStatement.bind(W_YTD, W_ID));
    }

    private void updateDistrict(int D_W_ID, int D_ID, BigDecimal paymentAmount, Row districtRelation) {
        //Update the district (C W ID,C D ID) by incrementing D YTD by PAYMENT
        BigDecimal D_YTD = districtRelation.getDecimal("D_YTD").add(paymentAmount);
        session.execute(updateDistrictYTDStatement.bind(D_YTD, D_W_ID, D_ID));
    }

    private void updateCustomer(int C_W_ID, int C_D_ID, int C_ID, BigDecimal paymentAmount,
                                Row customerRelation) {
        /*
         * Update the customer (C W ID, C D ID, C ID) as follows:
         * Decrement C BALANCE by PAYMENT
         * Increment C YTD PAYMENT by PAYMENT
         * Increment C PAYMENT CNT by 1
         */

        // Decrement C BALANCE by PAYMENT
        BigDecimal C_BALANCE = customerRelation.getDecimal("C_BALANCE").subtract(paymentAmount);

        // Increment C YTD PAYMENT by PAYMENT
        float C_YTD_PAYMENT = customerRelation.getFloat("C_YTD_PAYMENT") + paymentAmount.floatValue();

        // Increment C PAYMENT CNT by 1
        int C_PAYMENT_CNT = customerRelation.getInt("C_PAYMENT_CNT") + 1;

        session.execute(updateCustomerByPaymentStatement.bind(C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT,
                C_W_ID, C_D_ID, C_ID));
    }

    /* Output methods */

    private void outputPaymentTransactionResults(Row warehouse, Row district, Row customer,
                                                 float paymentAmount) {
        printCustomerDetails(customer);
        printWarehouseAddress(warehouse);
        printDistrictAddress(district);
        printPaymentAmount(paymentAmount);
    }

    private void printCustomerDetails(Row customer) {
        System.out.println(String.format(MESSAGE_CUSTOMER,
                customer.getInt("C_W_ID"),
                customer.getInt("C_D_ID"),
                customer.getInt("C_ID"),

                customer.getString("C_FIRST"),
                customer.getString("C_MIDDLE"),
                customer.getString("C_LAST"),

                customer.getString("C_STREET_1"),
                customer.getString("C_STREET_2"),
                customer.getString("C_CITY"),
                customer.getString("C_STATE"),
                customer.getString("C_ZIP"),

                customer.getString("C_PHONE"),
                customer.getTimestamp("C_SINCE"),

                customer.getString("C_CREDIT"),
                customer.getDecimal("C_CREDIT_LIM"),
                customer.getDecimal("C_DISCOUNT"),
                customer.getDecimal("C_BALANCE")));
    }

    private void printWarehouseAddress(Row warehouse) {
        System.out.println(String.format(MESSAGE_WAREHOUSE,
                warehouse.getString("W_STREET_1"),
                warehouse.getString("W_STREET_2"),
                warehouse.getString("W_CITY"),
                warehouse.getString("W_STATE"),
                warehouse.getString("W_ZIP")));
    }

    private void printDistrictAddress(Row district) {
        System.out.println(String.format(MESSAGE_DISTRICT,
                district.getString("D_STREET_1"),
                district.getString("D_STREET_2"),
                district.getString("D_CITY"),
                district.getString("D_STATE"),
                district.getString("D_ZIP")));
    }

    private void printPaymentAmount(float paymentAmount) {
        System.out.println(String.format(MESSAGE_PAYMENT, paymentAmount));
    }

}
