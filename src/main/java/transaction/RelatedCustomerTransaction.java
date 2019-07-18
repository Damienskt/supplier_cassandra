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

    private String KEY_SPACE_WITH_DOT;
    private Session session;

    private String ITEMS_CUST_QUERY;
    private String CUSTS_ITEM_QUERY;
    private String ORDER_CUST_QUERY;
    private String CUST_ID_QUERY;

    public RelatedCustomerTransaction(Session session, String keySpace) {
        this.KEY_SPACE_WITH_DOT = keySpace + ".";

        this.ITEMS_CUST_QUERY = //retrieve Items ordered by main customer
                "SELECT ol_i_id "
                        + "FROM "+ KEY_SPACE_WITH_DOT +"order_line "
                        + "WHERE ol_w_id=? AND ol_d_id=? AND ol_o_id = ?;";
        this.CUSTS_ITEM_QUERY = //retrieve customers that have the same item
                "SELECT ol_w_id, ol_o_id, ol_d_id "
                        + "FROM "+ KEY_SPACE_WITH_DOT +"ordered_items "
                        + "WHERE ol_i_id = ?;";
        this.ORDER_CUST_QUERY = //retrieve order by main customer
                "SELECT o_id "
                        + "FROM "+ KEY_SPACE_WITH_DOT + "order_partitioned_by_customer "
                        + "WHERE o_w_id=? AND o_d_id=? AND o_c_id = ?;";
        this.CUST_ID_QUERY = //retrieve c_id from order
                "SELECT o_c_id "
                        + "FROM "+ KEY_SPACE_WITH_DOT + "orders "
                        + "WHERE o_w_id=? AND o_d_id=? AND o_id = ?;";

        this.session = session;
        itemsOfCustCql = session.prepare(ITEMS_CUST_QUERY);
        custWithItemCql = session.prepare(CUSTS_ITEM_QUERY);
        retrieveCustIDCql = session.prepare(CUST_ID_QUERY);
        retrieveOrderIDCql = session.prepare(ORDER_CUST_QUERY);
    }

    public void relatedCustomer (int w_id, int d_id, int c_id) {
        ResultSet resultSet2 = session.execute(retrieveOrderIDCql.bind(w_id,d_id,c_id)); //get order ids of the customer in question
        List<Row> orderIDs = resultSet2.all();
        HashMap <String,Integer> customerWithItem; //store the similar count of each unqiue order
        HashMap <String,ArrayList<Integer>> keyToCust; //store the identifiers w_id d_id and o_id needed
        for(Row orderid : orderIDs) { //for all order ids get the list of items
            customerWithItem = new HashMap();
            keyToCust = new HashMap();
            System.out.println("orderId: " + orderid.getInt("o_id"));
            ResultSet resultSet = session.execute(itemsOfCustCql.bind(w_id,d_id, orderid.getInt("o_id"))); //get all items for each order made by the main customer
            List<Row> itemsByCust = resultSet.all();
            for(Row item : itemsByCust){//for each order use all item id to get other customers (based on w o and d)
                //System.out.println("item id: "+ item.getInt("ol_i_id"));
                ResultSet resultSet1 = session.execute(custWithItemCql.bind(item.getInt("ol_i_id")));
                List<Row> custList = resultSet1.all();
                for(Row cusN : custList) {
                    //System.out.println("w id: "+ cusN.getInt("ol_w_id") + ", d id: "+cusN.getInt("ol_d_id") + ", o id:" + cusN.getInt("ol_o_id"));
                    
                    String key = "w_id: "+ cusN.getInt("ol_w_id") + " d_id: "+ cusN.getInt("ol_d_id") + " o_id"+ cusN.getInt("ol_o_id");
                    ArrayList<Integer> addArrayList = new ArrayList<Integer>();
                    addArrayList.add(0,cusN.getInt("ol_w_id")); //index 0 is w_id
                    addArrayList.add(1,cusN.getInt("ol_d_id")); //index 1 is d_id
                    addArrayList.add(2,cusN.getInt("ol_o_id")); //index 2 is o_id

                    if(customerWithItem.containsKey(key) && customerWithItem.get(key).intValue()==1) { //add one value for existing key to account for the similar item
                        customerWithItem.put(key, customerWithItem.get(key).intValue() + 1);
                        keyToCust.put(key, addArrayList);
                    }
                    else { //if key doesnt exist create one
                        customerWithItem.put(key, 1);
                    }
                }
            }
            printRelatedCust(keyToCust,w_id,d_id,c_id);
        }
    }

    private void printRelatedCust(HashMap<String,ArrayList<Integer>> keyToCust,int wid,int did, int cid){
        Iterator it = keyToCust.entrySet().iterator();
        while (it.hasNext()) {
            HashMap.Entry pair = (HashMap.Entry)it.next();
            ResultSet resultSet3 = session.execute(retrieveCustIDCql.bind(keyToCust.get(pair.getKey()).get(0),keyToCust.get(pair.getKey()).get(1),keyToCust.get(pair.getKey()).get(2)));
            Row result = (resultSet3.all()).get(0);
            if(wid!=keyToCust.get(pair.getKey()).get(0) && did!=keyToCust.get(pair.getKey()).get(1) && cid!=result.getInt("o_c_id"))
                System.out.println("cus_id: "+result.getInt("o_c_id")+" district_id: "+keyToCust.get(pair.getKey()).get(1)+" warehouse_id: "+keyToCust.get(pair.getKey()).get(0));
            it.remove(); // avoids a ConcurrentModificationException
        }
    }

}
