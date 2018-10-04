package transaction;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import java.math.BigDecimal;
import java.util.List;

import constant.Table;

public class StockLevelTransaction {
    private Session session;
    private PreparedStatement selectDistrictNextOrderIdStatement;
    private PreparedStatement selectLastOrderLineIdStatement;
    private PreparedStatement checkIfItemBelowThresholdStatement;

    private int counter;

    private static final String KEY_SPACE_WITH_DOT = Table.KEY_SPACE + ".";

    private static final String SELECT_DISTRICT_NEXT_O_ID =
            "SELECT D_NEXT_O_ID "
                    + "FROM " + KEY_SPACE_WITH_DOT + Table.TABLE_DISTRICT
                    + " WHERE D_W_ID = ? AND D_ID = ?;";

    private static final String SELECT_LAST_OL_I_D =
            "SELECT OL_I_ID "
                    + "FROM " + KEY_SPACE_WITH_DOT + Table.TABLE_ORDERLINE
                    + " WHERE OL_D_ID = ? "
                    + "AND OL_W_ID = ? "
                    + "AND OL_O_ID = ?;";

    // If return some count item is below the threshold level
    private static final String CHECK_IF_ITEM_BELOW_THRESHOLD =
            "SELECT COUNT(*) "
                    + "FROM " + KEY_SPACE_WITH_DOT + Table.TABLE_STOCK
                    + " WHERE S_W_ID = ? "
                    + "AND S_I_ID = ? "
                    + "AND S_QUANTITY < ? "
                    + "ALLOW FILTERING;";

    public StockLevelTransaction(Session session) {
        this.session = session;
        this.counter = 0;
        this.selectDistrictNextOrderIdStatement = session.prepare(SELECT_DISTRICT_NEXT_O_ID);
        this.selectLastOrderLineIdStatement = session.prepare(SELECT_LAST_OL_I_D);
        this.checkIfItemBelowThresholdStatement = session.prepare(CHECK_IF_ITEM_BELOW_THRESHOLD);
    }

    public void processStockLevelTransaction(int W_ID, int D_ID, BigDecimal T, int L) {
        /*
            Processing steps:
            1. Let N denote the value of the next available order number D NEXT O ID for district (W ID,D ID)
            2. Let S denote the set of items from the last L orders for district (W ID,D ID); i.e.,
            S = {t.OL I ID | t ∈ Order-Line, t.OL D ID = D ID, t.OL W ID = W ID, t.OL O ID ∈ [N−L,N)}
            3. Output the total number of items in S where its stock quantity at W ID is below the threshold;
            i.e., S QUANTITY < T
         */

        int D_NEXT_O_ID = -1;
        ResultSet rs = session.execute(selectDistrictNextOrderIdStatement.bind(W_ID, D_ID));
        List<Row> district = rs.all();
        if (!district.isEmpty()) {
            Row targetDistrict = district.get(0);
            D_NEXT_O_ID = targetDistrict.getInt("D_NEXT_O_ID");
        }

        for (int i = (D_NEXT_O_ID + 1 - L); i <= D_NEXT_O_ID; i++) {
            rs = session.execute(selectLastOrderLineIdStatement.bind(D_ID, W_ID, i));
            List<Row> itemSetS = rs.all();

            if (!itemSetS.isEmpty()) {
                for (Row item: itemSetS) {
                    int OL_I_ID = item.getInt("OL_I_ID");
                    rs = session.execute(checkIfItemBelowThresholdStatement.bind(W_ID, OL_I_ID, T));
                    List<Row> count = rs.all();

                    if (!count.isEmpty()) {
                        counter++;
                    }
                }
            }
        }

        System.out.println(counter);
    }
}