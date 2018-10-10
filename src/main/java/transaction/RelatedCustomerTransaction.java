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
    private static final String KEY_SPACE_WITH_DOT = Table.KEY_SPACE + ".";
    private Session session;

    private static final String ITEMS_CUST_QUERY = //retrieve Items ordered by main customer
            "SELECT oi_i_id "
                    + "FROM "+ KEY_SPACE_WITH_DOT +"order_line "
                    + "WHERE c_w_id=? AND c_d_id=? AND c_id = ?;";
    private static final String CUSTS_ITEM_QUERY = //retrieve customers that have the same item
            "SELECT oi_w_id, oi_c_id, oi_d_id"
                    + "FROM "+ KEY_SPACE_WITH_DOT +"ordered_items "
                    + "WHERE oi_i_id = ?;";

    public RelatedCustomerTransaction(Session session) {
        this.session = session;
        itemsOfCustCql = session.prepare(ITEMS_CUST_QUERY);
        custWithItemCql = session.prepare(CUSTS_ITEM_QUERY);
    }

    public void relatedCustomer (int w_id, int d_id, int c_id) {
        ResultSet resultSet = session.execute(itemsOfCustCql.bind(w_id,c_id,d_id));
        HashMap <Integer,Integer> customerWithItem = new HashMap<Integer, Integer>();
        HashMap <Integer,ArrayList<Integer>> keyToCust = new HashMap<Integer, ArrayList<Integer>>();
        List<Row> itemsByCust = resultSet.all();
        for(Row item : itemsByCust){
            ResultSet resultSet1 = session.execute(custWithItemCql.bind(item.getInt("oi_i_id")));
            List<Row> custList = resultSet1.all();
            for(Row cusN : custList) {
                Key custKey = new Key(cusN.getInt("oi_w_id"), cusN.getInt("oi_d_id"), cusN.getInt("oi_c_id"));
                int key = custKey.hashCode();
                ArrayList<Integer> addArrayList = new ArrayList<Integer>();
                addArrayList.add(0,cusN.getInt("oi_w_id")); //index 0 is w_id
                addArrayList.add(1,cusN.getInt("oi_d_id")); //index 1 is d_id
                addArrayList.add(2,cusN.getInt("oi_c_id")); //index 2 is c_id
                keyToCust.put(key,addArrayList);

                if(customerWithItem.containsKey(key))
                    if(customerWithItem.get(key).equals(1))
                        customerWithItem.put(key, customerWithItem.get(key)+1);

                else {
                    customerWithItem.put(key, 1);
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
                int c_id = keyToCust.get(pair.getKey()).get(2);
                System.out.println("cus_id: "+c_id+" district_id: "+d_id+" warehouse_id: "+w_id);
            }
            it.remove(); // avoids a ConcurrentModificationException
        }
    }

}
