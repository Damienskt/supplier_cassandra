package transaction;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

import constant.Table;

public class NewOrderTransaction {
    private Session session;

    private PreparedStatement selectDistrictStatement;
    private PreparedStatement selectWarehouseTaxStatement;
    private PreparedStatement selectStockStatement;
    private PreparedStatement selectItemStatement;

    private PreparedStatement insertOrderStatement;
    private PreparedStatement insertOrderLineStatement;

    private static final String SELECT_DISTRICT = " SELECT D_TAX, D_NEXT_O_ID FROM "
            + Table.TABLE_DISTRICT + " WHERE D_W_ID = ? AND D_ID = ?; ";
    private static final String SELECT_WAREHOUSE = " SELECT W_TAX FROM "
            + Table.TABLE_WAREHOUSE + " WHERE W_ID = ?; ";
    private static final String SELECT_STOCK = " SELECT * FROM "
            + Table.TABLE_STOCK + " WHERE S_W_ID = ? AND S_I_ID = ?; ";
    private static final String SELECT_ITEM = " SELECT I_NAME, I_PRICE FROM "
            + Table.TABLE_ITEM + " WHERE I_ID = ?; ";

    private static final String INSERT_ORDER = " INSERT INTO " + Table.TABLE_ORDER + " ("
            + " O_W_ID, O_D_ID, O_ID, O_C_ID, O_CARRIER_ID, "
            + " O_OL_CNT, O_ALL_LOCAL, O_ENTRY_D ) "
            + " VALUES (?, ?, ?, ?, ?, ?, ?, ?); ";
    private static final String INSERT_ORDERLINE = " INSERT INTO " + Table.TABLE_ORDERLINE + " ("
            + " OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER, OL_I_ID, "
            + " OL_DELIVERY_D, OL_AMOUNT, OL_SUPPLY_W_ID, OL_QUANTITY, OL_DIST_INFO ) "
            + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?); ";

    private static final String UPDATE_DISTRICT_NEXT_O_ID = " UPDATE " + Table.TABLE_DISTRICT
            + " SET D_NEXT_O_ID = ? WHERE D_W_ID = ? AND D_ID = ?; ";
    private static final String UPDATE_CUSTOMER

    public NewOrderTransaction(Session session) {
        this.session = session;
        this.selectDistrictStatement = session.prepare(SELECT_DISTRICT);
        this.selectWarehouseTaxStatement = session.prepare(SELECT_WAREHOUSE);
        this.selectStockStatement = session.prepare(SELECT_STOCK);
        this.selectItemStatement = session.prepare(SELECT_ITEM);

        this.insertOrderStatement = session.prepare(INSERT_ORDER);
        this.insertOrderLineStatement = session.prepare(INSERT_ORDERLINE);
    }

    public void processOrder(int w_id, int d_id, int c_id, int numItems,
                             int[] itemNum, int[] supplierWarehouse, int[] qty) {

    }
}
