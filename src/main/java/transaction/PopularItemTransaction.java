package transaction;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import java.math.BigDecimal;
import java.util.List;
import java.util.ArrayList;
import java.text.SimpleDateFormat;

import constant.Table;

public class PopularItemTransaction {
    private PreparedStatement orderWithItemCql;
    private PreparedStatement itemNameCql;
    private PreparedStatement customerNameCql;
    private PreparedStatement lastOrdersCql;
    private PreparedStatement maxQuantityCql;
    private PreparedStatement popularItemCql;
    private static final String KEY_SPACE_WITH_DOT = Table.KEY_SPACE + ".";

    private Session session;
    /* popular items */
    private static final String SELECT_LAST_ORDERS =
            "SELECT o_id, o_c_id, o_entry_d "
                    + "FROM "+ KEY_SPACE_WITH_DOT +"orders "
                    + "WHERE o_w_id = ? AND o_d_id = ? "
                    + "ORDER BY o_id DESC "
                    + "LIMIT ? ALLOW FILTERING;";
    private static final String SELECT_MAX_QUANTITY =
            "SELECT MAX(ol_quantity) as ol_quantity "
                    + "FROM "+ KEY_SPACE_WITH_DOT +"order_line "
                    + "WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ?;";
    private static final String SELECT_ITEM_NAME =
            "SELECT i_name "
                    + "FROM "+ KEY_SPACE_WITH_DOT +"item "
                    + "WHERE i_id = ?;";
    private static final String SELECT_CUSTOMER_NAME =
            "SELECT c_first, c_middle, c_last "
                    + "FROM "+ KEY_SPACE_WITH_DOT +"customer "
                    + "WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?;";
    private static final String SELECT_POPULAR_ITEM =
            "SELECT ol_i_id, ol_quantity "
                    + "FROM "+ KEY_SPACE_WITH_DOT +"order_line "
                    + "WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ? AND ol_quantity = ? ALLOW FILTERING;";
    private static final String SELECT_ORDER_WITH_ITEM =
            "SELECT * "
                    + "FROM "+ KEY_SPACE_WITH_DOT +"order_line "
                    + "WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ? AND ol_i_id = ? ALLOW FILTERING;";

    public PopularItemTransaction(Session session) {
        this.session = session;
        /* popular items */
        orderWithItemCql = session.prepare(SELECT_ORDER_WITH_ITEM);
        itemNameCql = session.prepare(SELECT_ITEM_NAME);
        customerNameCql = session.prepare(SELECT_CUSTOMER_NAME);
        lastOrdersCql = session.prepare(SELECT_LAST_ORDERS);
        maxQuantityCql = session.prepare(SELECT_MAX_QUANTITY);
        popularItemCql = session.prepare(SELECT_POPULAR_ITEM);
    }

    /* Start of public methods */

    /**
     *
     * @param wId : used for customer identifier
     * @param dId : used for customer identifier
     * @param numOfOrders : number of lastest order to be considered
     */

    public void popularItem(int wId, int dId, int numOfOrders) {
        List<Row> lastOrders = selectLastOrders(wId, dId, numOfOrders);
        int num = lastOrders.size();
//        List<Row> customers = new List();
        List<List<Row>> popularItemOfOrder = new ArrayList();
        List<Integer> popularItems = new ArrayList();
        List<String> popularItemName = new ArrayList();
        for (int i = 0; i < num; i++) {
            int orderId = lastOrders.get(i).getInt("o_id");
            //System.out.println("order ids " + orderId);
//            cId = lastOrders.get(i)[1];
//            customers.add(getCustomer(wId, dId, cId));
            List<Row>popularItem = getPopularItem(wId, dId, orderId);
            popularItemOfOrder.add(popularItem);
            //System.out.println("item ids:");
            for (Row item: popularItem) {
                int itemId = item.getInt("ol_i_id");
                //System.out.println("itemId " + itemId);
                if (!popularItems.contains(itemId)) {
                    popularItems.add(itemId);
                    String item_name = getItemName(itemId);
                    popularItemName.add(item_name);
                }
            }
        }
        int[] percentage = new int[popularItems.size()];
//        String[] itemName = new String[popularItems.size()]
        //System.out.println("percentage: " + popularItems.size());
        for (int i = 0; i < popularItems.size(); i++){
            int itemId = popularItems.get(i);
//            orderId = lastOrders.get(i)[0];
//            itemName[i] = getItemName(itemId);
            percentage[i] = getPercentage(wId, dId, lastOrders, itemId);
        }
        outputPopularItems(wId, dId, numOfOrders, lastOrders, popularItemOfOrder,
                popularItemName, percentage);
    }


    /*  End of public methods */
    /*  popular items */
    private List<Row> selectLastOrders(final int wId, final int dId, final int numOfOrders) {
        ResultSet resultSet = session.execute(lastOrdersCql.bind(wId, dId, numOfOrders));
        List<Row> lastOrders = resultSet.all();
        return lastOrders;
    }

//    private void getCustomer(final int wId, final int dId, final int cId) {
//        ResultSet resultSet = session.execute(selectCustomerStmt.bind(wId, dId, cId));
//        List<Row> customers = resultSet.all();
//
//        if(!customers.isEmpty()) {
//            return customers.get(0);
//        }
//    }

    private List<Row> getPopularItem(final int wId, final int dId, final int orderId) {
        ResultSet resultSet1 = session.execute(maxQuantityCql.bind(wId, dId, orderId));
        BigDecimal maxQuantity= (resultSet1.all()).get(0).getDecimal("ol_quantity");

        ResultSet resultSet2 = session.execute(popularItemCql.bind(wId, dId, orderId, maxQuantity));
        List<Row> popularItem = resultSet2.all();
        return popularItem;
    }

//    private void getItemName(final int itemId){
//        ResultSet resultSet = session.execute(itemNameCql.bind(itemId);
//        List<Row> itemName = resultSet.all();
//        return itemName.get(0);
//    }

    private int getPercentage(final int wId, final int dId, final List<Row> lastOrders, final int itemId) {
        int count = 0;
        for (int i = 0; i < lastOrders.size(); i++) {
            int orderId = lastOrders.get(i).getInt("o_id");
            ResultSet resultSet = session.execute(orderWithItemCql.bind(wId, dId, orderId, itemId));
            List<Row> result = resultSet.all();
            if (!result.isEmpty()){
                count++;
            }
        }
        //System.out.println("getPercentage");
        //System.out.println(count);
        return count;

    }
    private String getItemName(int item_id) {
        ResultSet resultSet2 = session.execute(itemNameCql.bind(item_id));
        List<Row> result = resultSet2.all();
        //System.out.println("item_name: "+result.get(0).getString("i_name"));
        return result.get(0).getString("i_name");
    }
    private List<Row> getCustomerName (int w_id, int d_id, int c_id) {
        ResultSet resultSet = session.execute(customerNameCql.bind(w_id,d_id,c_id));
        List<Row> result = resultSet.all();
        return result;
    }

    private void outputPopularItems(final int wId, final int dId, final int numOfOrders, List<Row> lastOrders,
                                    List<List<Row>> popularItemOfOrder, List<String> popularItemName, int[] percentage){
        System.out.println("WId: " + wId + " DId: " + dId);
        System.out.println("number of orders been examined: " + numOfOrders);
        for (int i = 0; i < numOfOrders; i++) {
            Row order = lastOrders.get(i);
            int c_id = order.getInt("o_c_id");
            Row customerName = getCustomerName(wId,dId,c_id).get(0);
            System.out.println("order Id: " + order.getInt("o_id"));
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); //Or whatever format fits best your needs.
            String dateStr = sdf.format(order.getTimestamp("o_entry_d"));
            System.out.println("entry date and time: " + dateStr);
            System.out.println("customer name: " + customerName.getString("c_first") + " "
                    + customerName.getString("c_middle") + " " + customerName.getString("c_last"));
            for (Row pitem: popularItemOfOrder.get(i)) {
                int itemId = pitem.getInt("ol_i_id");
                System.out.println("item name: " + getItemName(itemId));
                System.out.println("item quantity: " + (pitem.getDecimal("ol_quantity")).intValue());
            }
        }
        for (int i = 0; i < popularItemName.size(); i++) {
            System.out.println("popular item percentage:");
            System.out.println("item name: " + popularItemName.get(i));
            double per = percentage[i] * 100.0 / numOfOrders;
            System.out.println(String.format("percentage: %.2f", per));
        }

    }

    /*  End of private methods */
}
