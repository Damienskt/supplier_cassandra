package transaction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import constant.Table;

public class RelatedCustomerTransaction {
    private PreparedStatement itemsOfCustCql;
    private PreparedStatement custWithItemCql;
    private PreparedStatement retrieveCustIDCql;
    private PreparedStatement retrieveOrderIDCql;

    private static final String KEY_SPACE_WITH_DOT = Table.KEY_SPACE + ".";
    private Session session;

    private static final String ITEMS_CUST_QUERY = //retrieve Items ordered by main customer
            "SELECT ol_i_id "
                    + "FROM "+ KEY_SPACE_WITH_DOT +"order_line "
                    + "WHERE ol_w_id=? AND ol_d_id=? AND ol_o_id = ?;";
    private static final String CUSTS_ITEM_QUERY = //retrieve customers that have the same item
            "SELECT ol_w_id, ol_o_id, ol_d_id "
                    + "FROM "+ KEY_SPACE_WITH_DOT +"ordered_items "
                    + "WHERE ol_i_id = ?;";
    private static final String ORDER_CUST_QUERY = //retrieve order by main customer
            "SELECT o_id "
                    + "FROM "+ KEY_SPACE_WITH_DOT + "order_partitioned_by_customer "
                    + "WHERE o_w_id=? AND o_d_id=? AND o_c_id = ?;";
    private static final String CUST_ID_QUERY = //retrieve c_id from order
            "SELECT o_c_id "
                    + "FROM "+ KEY_SPACE_WITH_DOT + "orders "
                    + "WHERE o_w_id=? AND o_d_id=? AND o_id = ?;";
    public RelatedCustomerTransaction(Session session) {
        this.session = session;
        itemsOfCustCql = session.prepare(ITEMS_CUST_QUERY);
        custWithItemCql = session.prepare(CUSTS_ITEM_QUERY);
        retrieveCustIDCql = session.prepare(CUST_ID_QUERY);
        retrieveOrderIDCql = session.prepare(ORDER_CUST_QUERY);
    }

    public void relatedCustomer (int w_id, int d_id, int c_id) {
        ResultSet resultSet2 = session.execute(retrieveOrderIDCql.bind(w_id,d_id,c_id)); //get order ids
        List<Row> orderIDs = resultSet2.all();
        List<Row> itemsByCust = null;
        HashMap <Integer,Integer> customerWithItem = new HashMap<Integer, Integer>();
        HashMap <Integer,ArrayList<Integer>> keyToCust = new HashMap<Integer, ArrayList<Integer>>();
        for(Row orderid : orderIDs) {

            System.out.println("orderId: " + orderid.getInt("o_id"));
            ResultSet resultSet = session.execute(itemsOfCustCql.bind(w_id,d_id, orderid.getInt("o_id"))); //get all items for each order made by the main customer
            itemsByCust = resultSet.all();
            for(Row item : itemsByCust){//for each order use all item id to get other customers (based on w o and d)
                ResultSet resultSet1 = session.execute(custWithItemCql.bind(item.getInt("ol_i_id")));
                List<Row> custList = resultSet1.all();
                for(Row cusN : custList) {
                    Key custKey = new Key(cusN.getInt("ol_w_id"), cusN.getInt("ol_d_id"), cusN.getInt("ol_o_id"));
                    int key = custKey.hashCode();
                    ArrayList<Integer> addArrayList = new ArrayList<Integer>();
                    addArrayList.add(0,cusN.getInt("ol_w_id")); //index 0 is w_id
                    addArrayList.add(1,cusN.getInt("ol_d_id")); //index 1 is d_id
                    addArrayList.add(2,cusN.getInt("ol_o_id")); //index 2 is o_id
                    keyToCust.put(key,addArrayList);

                    if(customerWithItem.containsKey(key) && !customerWithItem.get(key).equals(2))
                            customerWithItem.put(key, customerWithItem.get(key)+1);
                    else {
                            customerWithItem.put(key, 1);
                    }
                }
            }
        }
        printRelatedCust(customerWithItem, keyToCust);
    }

    private void printRelatedCust(HashMap<Integer,Integer> custWithItem, HashMap<Integer,ArrayList<Integer>> keyToCust){
        Iterator it = custWithItem.entrySet().iterator();
        while (it.hasNext()) {
            HashMap.Entry pair = (HashMap.Entry)it.next();
            if(pair.getValue().equals(2)) {
                int w_id = keyToCust.get(pair.getKey()).get(0);
                int d_id = keyToCust.get(pair.getKey()).get(1);
                int o_id = keyToCust.get(pair.getKey()).get(2);
                ResultSet resultSet3 = session.execute(retrieveCustIDCql.bind(w_id,d_id,o_id));
                Row result = (resultSet3.all()).get(0);
                System.out.println("cus_id: "+result.getInt("o_c_id")+" district_id: "+d_id+" warehouse_id: "+w_id);
            }
            it.remove(); // avoids a ConcurrentModificationException
        }
    }

}
