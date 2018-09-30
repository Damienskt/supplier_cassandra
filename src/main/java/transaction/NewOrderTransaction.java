package transaction;

import java.math.BigDecimal;
import java.util.Date;
import java.util.List;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import constant.Table;

public class NewOrderTransaction {
    private Session session;

    private PreparedStatement selectDistrictStatement;
    private PreparedStatement selectWarehouseTaxStatement;
    private PreparedStatement selectStockStatement;
    private PreparedStatement selectItemStatement;
    private PreparedStatement selectCustomerStatement;

    private PreparedStatement insertOrderStatement;
    private PreparedStatement insertOrderLineStatement;

    private PreparedStatement updateCustomerLastOrderStatement;
    private PreparedStatement updateDistrictNextOrderStatement;
    private PreparedStatement updateStockStatement;

    private static final String KEY_SPACE_WITH_DOT = Table.KEY_SPACE + ".";

    private static final String SELECT_DISTRICT = " SELECT D_TAX, D_NEXT_O_ID FROM "
            + KEY_SPACE_WITH_DOT + Table.TABLE_DISTRICT + " WHERE D_W_ID = ? AND D_ID = ?; ";
    private static final String SELECT_WAREHOUSE = " SELECT W_TAX FROM "
            + KEY_SPACE_WITH_DOT + Table.TABLE_WAREHOUSE + " WHERE W_ID = ?; ";
    private static final String SELECT_STOCK = " SELECT * FROM "
            + KEY_SPACE_WITH_DOT + Table.TABLE_STOCK + " WHERE S_W_ID = ? AND S_I_ID = ?; ";
    private static final String SELECT_ITEM = " SELECT I_NAME, I_PRICE FROM "
            + KEY_SPACE_WITH_DOT + Table.TABLE_ITEM + " WHERE I_ID = ?; ";
    private static final String SELECT_CUSTOMER = " SELECT C_LAST, C_CREDIT, C_DISCOUNT FROM "
            + KEY_SPACE_WITH_DOT + Table.TABLE_CUSTOMER + " WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?; ";

    private static final String INSERT_ORDER = " INSERT INTO " + KEY_SPACE_WITH_DOT + Table.TABLE_ORDER + " ("
            + " O_W_ID, O_D_ID, O_ID, O_C_ID, O_CARRIER_ID, "
            + " O_OL_CNT, O_ALL_LOCAL, O_ENTRY_D ) "
            + " VALUES (?, ?, ?, ?, ?, ?, ?, ?); ";
    private static final String INSERT_ORDERLINE = " INSERT INTO " + KEY_SPACE_WITH_DOT + Table.TABLE_ORDERLINE + " ("
            + " OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER, OL_I_ID, "
            + " OL_DELIVERY_D, OL_AMOUNT, OL_SUPPLY_W_ID, OL_QUANTITY, OL_DIST_INFO ) "
            + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?); ";

    private static final String UPDATE_CUSTOMER_LAST_O_ID = " UPDATE " + KEY_SPACE_WITH_DOT + Table.TABLE_CUSTOMER
            + " SET C_LAST_O_ID = ?, C_O_ENTRY_D = ?, C_O_CARRIER_ID = ? "
            + " WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?;";
    private static final String UPDATE_DISTRICT_NEXT_O_ID = " UPDATE " + KEY_SPACE_WITH_DOT + Table.TABLE_DISTRICT
            + " SET D_NEXT_O_ID = ? WHERE D_W_ID = ? AND D_ID = ?; ";
    private static final String UPDATE_STOCK = " UPDATE " + KEY_SPACE_WITH_DOT + Table.TABLE_STOCK
            + " SET S_QUANTITY = ?, S_YTD = ?, S_ORDER_CNT = ?, S_REMOTE_CNT = ? "
            + " WHERE S_W_ID = ? AND S_I_ID = ?; ";

    public NewOrderTransaction(Session session) {
        this.session = session;
        this.selectDistrictStatement = session.prepare(SELECT_DISTRICT);
        this.selectWarehouseTaxStatement = session.prepare(SELECT_WAREHOUSE);
        this.selectStockStatement = session.prepare(SELECT_STOCK);
        this.selectItemStatement = session.prepare(SELECT_ITEM);
        this.selectCustomerStatement = session.prepare(SELECT_CUSTOMER);

        this.insertOrderStatement = session.prepare(INSERT_ORDER);
        this.insertOrderLineStatement = session.prepare(INSERT_ORDERLINE);

        this.updateCustomerLastOrderStatement = session.prepare(UPDATE_CUSTOMER_LAST_O_ID);
        this.updateDistrictNextOrderStatement = session.prepare(UPDATE_DISTRICT_NEXT_O_ID);
        this.updateStockStatement = session.prepare(UPDATE_STOCK);
    }

    public void processOrder(int wId, int dId, int cId, int numItems,
                             int[] itemNum, int[] supplierWarehouse, int[] qty) {
        Row warehouse = getWarehouse(wId);
        Row district = getDistrict(wId, dId);
        Row customer = getCustomer(wId, dId, cId);

        double dTax = district.getDecimal("D_TAX").doubleValue();
        double wTax = warehouse.getDecimal("W_TAX").doubleValue();
        int nextOId = district.getInt("D_NEXT_O_ID");
        updateDistrictNextOrderNumber(nextOId + 1, wId, dId);

        Date currentDate = new Date();
        BigDecimal allLocal = new BigDecimal(1);

        String[] itemOutput = new String[numItems];

        for (int i = 0; i < numItems; i++) {
            if (supplierWarehouse[i] != wId) {
                allLocal = new BigDecimal(0);
                break;
            }
        }
        createNewOrder(nextOId, wId, dId, cId, new BigDecimal(numItems), currentDate, allLocal);
        updateCustomerLastOrder(nextOId, wId, dId, cId, currentDate);

        double totalAmount = 0;
        for (int i = 0; i < numItems; i++) {
            int iId = itemNum[i];
            int iWId = supplierWarehouse[i];
            int quantity = qty[i];

            Row stock = getStock(iWId, iId);
            double adjustedQuantity = stock.getDecimal("S_QUANTITY").doubleValue() - quantity;
            while (adjustedQuantity < 10) {
                adjustedQuantity += 100;
            }

            // Update stock
            BigDecimal adjQuantity = new BigDecimal(adjustedQuantity);
            BigDecimal ytd = new BigDecimal(stock.getDecimal("S_YTD").doubleValue() + quantity);
            int orderCount = stock.getInt("S_ORDER_CNT") + 1;
            int remoteCount = stock.getInt("S_REMOTE_CNT");
            if (iWId != wId) {
                remoteCount++;
            }
            updateStock(iWId, iId, adjQuantity, ytd, orderCount, remoteCount);

            Row item = getItem(iId);
            BigDecimal itemAmount = item.getDecimal("I_PRICE").multiply(new BigDecimal(quantity));
            totalAmount += itemAmount.doubleValue();
            String distInfo = stock.getString(getDistrictStringId(dId));
            createNewOrderLine(wId, dId, nextOId, i+1, iId, itemAmount, iWId, new BigDecimal(quantity), distInfo);

            itemOutput[i] = "" + (i+1) + "\t" + item.getString("I_NAME") + "\t" + iWId + " " + quantity
                    + "\t" + itemAmount + "\t" + adjustedQuantity;
        }

        totalAmount = totalAmount * (1 + dTax + wTax) * (1 - customer.getDecimal("C_DISCOUNT").doubleValue());

        System.out.println("Customer (" + wId + ", " + dId + ", " + cId + ")"
                + " C_LAST: " + customer.getString("C_LAST")
                + " C_CREDIT: " + customer.getString("C_CREDIT")
                + " C_DISCOUNT: " + customer.getDecimal("C_DISCOUNT"));
        System.out.println("Warehouse tax: " + wTax + ", district tax: " + dTax);
        System.out.println("Order number: " + nextOId + ", entry date: " + currentDate);
        System.out.println("Number of items: " + numItems + ", total amount: " + totalAmount);
        for (String s : itemOutput) {
            System.out.println(s);
        }
    }

    private Row getWarehouse(int wId) {
        ResultSet rs = session.execute(selectWarehouseTaxStatement.bind(wId));
        List<Row> warehouses = rs.all();
        return (warehouses.isEmpty()) ? null : warehouses.get(0);
    }

    private Row getDistrict(int wId, int dId) {
        ResultSet rs = session.execute(selectDistrictStatement.bind(wId, dId));
        List<Row> districts = rs.all();
        return (districts.isEmpty()) ? null : districts.get(0);
    }

    private Row getCustomer(int wId, int dId, int cId) {
        ResultSet rs = session.execute(selectCustomerStatement.bind(wId, dId, cId));
        List<Row> customers = rs.all();
        return (customers.isEmpty()) ? null : customers.get(0);
    }

    private Row getItem(int iId) {
        ResultSet rs = session.execute(selectItemStatement.bind(iId));
        List<Row> items = rs.all();
        return (items.isEmpty()) ? null : items.get(0);
    }

    private Row getStock(int wId, int iId) {
        ResultSet rs = session.execute(selectStockStatement.bind(wId, iId));
        List<Row> stocks = rs.all();
        return (stocks.isEmpty()) ? null : stocks.get(0);
    }

    private void updateDistrictNextOrderNumber(int nextOId, int wId, int dId) {
        session.execute(updateDistrictNextOrderStatement.bind(nextOId, wId, dId));
    }

    private void createNewOrder(int id, int wId, int dId, int cId, BigDecimal numItems,
            Date currentDate, BigDecimal allLocal) {
        session.execute(insertOrderStatement.bind(wId, dId, id, cId, -1, numItems, allLocal, currentDate));
    }

    private void updateCustomerLastOrder(int id, int wId, int dId, int cId, Date currentDate) {
        session.execute(updateCustomerLastOrderStatement.bind(id, currentDate, null, wId, dId, cId));
    }

    private void updateStock(int wId, int iId, BigDecimal adjQuantity, BigDecimal ytd,
            int orderCount, int remoteCount) {
        session.execute(updateStockStatement.bind(adjQuantity, ytd, orderCount, remoteCount, wId, iId));
    }

    private String getDistrictStringId(int dId) {
        if (dId < 10) {
            return "S_DIST_0" + dId;
        } else {
            return "S_DIST_10";
        }
    }

    private void createNewOrderLine(int wId, int dId, int oId, int olNum, int iId,
            BigDecimal itemAmount, int supplyWId, BigDecimal quantity, String distInfo) {
        session.execute(insertOrderLineStatement.bind(
                wId, dId, oId, olNum, iId, null, itemAmount, supplyWId, quantity, distInfo));
    }
}
