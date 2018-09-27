import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

import constant.Table;

public class Setup {
    static final String[] CONTACT_POINTS = {"localhost"};
    static final String KEY_SPACE = "wholesale_supplier";
    static final int REPLICATION_FACTOR = 1;

    private static final DateFormat DF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    private Session session;

    public static void main(String[] args) {
        Setup s = new Setup();
        s.run();
    }

    private void run() {
        Cluster cluster = Cluster.builder().addContactPoints(CONTACT_POINTS).build();
        session = cluster.connect();

        dropOldKeySpace();
        createKeySpace();
        createSchema();
        createView();
        loadData();

        System.out.println("Data has been successfully imported into the database.");

        session.close();
        cluster.close();
    }

    private void dropOldKeySpace() {
        String query = "DROP KEYSPACE IF EXISTS " + KEY_SPACE;
        session.execute(query);
    }

    private void createKeySpace() {
        String query = "CREATE KEYSPACE " + KEY_SPACE
                + " WITH REPLICATION = {"
                + " 'class' : 'SimpleStrategy',"
                + " 'replication_factor' : " + REPLICATION_FACTOR
                + " };";
        session.execute(query);
        System.out.println("Successfully created the keyspace: " + KEY_SPACE);
    }

    private void createSchema() {
        String createWarehouseQuery = "CREATE TABLE " + KEY_SPACE + "." + Table.TABLE_WAREHOUSE + " ("
                + " W_ID INT, "
                + " W_NAME TEXT, "
                + " W_STREET_1 TEXT, "
                + " W_STREET_2 TEXT, "
                + " W_CITY TEXT, "
                + " W_STATE TEXT, "
                + " W_ZIP TEXT, "
                + " W_TAX DECIMAL, "
                + " W_YTD DECIMAL, "
                + " PRIMARY KEY (W_ID) "
                + ");";

        String createDistrictQuery = "CREATE TABLE " + KEY_SPACE + "." + Table.TABLE_DISTRICT + " ("
                + " D_W_ID INT, "
                + " D_ID INT, "
                + " D_NAME TEXT, "
                + " D_STREET_1 TEXT, "
                + " D_STREET_2 TEXT, "
                + " D_CITY TEXT, "
                + " D_STATE TEXT, "
                + " D_ZIP TEXT, "
                + " D_TAX DECIMAL, "
                + " D_YTD DECIMAL, "
                + " D_NEXT_O_ID INT, "
                + " PRIMARY KEY (D_W_ID, D_ID) "
                + ");";

        String createCustomerQuery = "CREATE TABLE " + KEY_SPACE + "." + Table.TABLE_CUSTOMER + " ("
                + " C_W_ID INT, "
                + " C_D_ID INT, "
                + " C_ID INT, "
                + " C_FIRST TEXT, "
                + " C_MIDDLE TEXT, "
                + " C_LAST TEXT, "
                + " C_STREET_1 TEXT, "
                + " C_STREET_2 TEXT, "
                + " C_CITY TEXT, "
                + " C_STATE TEXT, "
                + " C_ZIP TEXT, "
                + " C_PHONE TEXT, "
                + " C_SINCE TIMESTAMP, "
                + " C_CREDIT TEXT, "
                + " C_CREDIT_LIM DECIMAL, "
                + " C_DISCOUNT DECIMAL, "
                + " C_BALANCE DECIMAL, "
                + " C_YTD_PAYMENT FLOAT, "
                + " C_PAYMENT_CNT INT, "
                + " C_DELIVERY_CNT INT, "
                + " C_DATA TEXT, "
                + " PRIMARY KEY (C_W_ID, C_D_ID, C_ID) "
                + ");";

        String createOrderQuery = "CREATE TABLE " + KEY_SPACE + "." + Table.TABLE_ORDER + " ("
                + " O_W_ID INT, "
                + " O_D_ID INT, "
                + " O_ID INT, "
                + " O_C_ID INT, "
                + " O_CARRIER_ID INT, "
                + " O_OL_CNT DECIMAL, "
                + " O_ALL_LOCAL DECIMAL, "
                + " O_ENTRY_D TIMESTAMP, "
                + " PRIMARY KEY (O_W_ID, O_D_ID, O_ID, O_C_ID) "
                + ");";

        String createItemQuery = "CREATE TABLE " + KEY_SPACE + "." + Table.TABLE_ITEM + " ("
                + " I_ID INT, "
                + " I_NAME TEXT, "
                + " I_PRICE DECIMAL, "
                + " I_IM_ID INT, "
                + " I_DATA TEXT, "
                + " PRIMARY KEY (I_ID) "
                + ");";

        String createOrderLineQuery = "CREATE TABLE " + KEY_SPACE + "." + Table.TABLE_ORDERLINE + " ("
                + " OL_W_ID INT, "
                + " OL_D_ID INT, "
                + " OL_O_ID INT, "
                + " OL_NUMBER INT, "
                + " OL_I_ID INT, "
                + " OL_DELIVERY_D TIMESTAMP, "
                + " OL_AMOUNT DECIMAL, "
                + " OL_SUPPLY_W_ID INT, "
                + " OL_QUANTITY DECIMAL, "
                + " OL_DIST_INFO TEXT, "
                + " PRIMARY KEY ((OL_W_ID, OL_D_ID, OL_O_ID), OL_NUMBER) "
                + " ) WITH CLUSTERING ORDER BY (OL_NUMBER ASC);";

        String createStockQuery = "CREATE TABLE " + KEY_SPACE + "." + Table.TABLE_STOCK + " ("
                + " S_W_ID INT, "
                + " S_I_ID INT, "
                + " S_QUANTITY DECIMAL, "
                + " S_YTD DECIMAL, "
                + " S_ORDER_CNT INT, "
                + " S_REMOTE_CNT INT, "
                + " S_DIST_01 TEXT, "
                + " S_DIST_02 TEXT, "
                + " S_DIST_03 TEXT, "
                + " S_DIST_04 TEXT, "
                + " S_DIST_05 TEXT, "
                + " S_DIST_06 TEXT, "
                + " S_DIST_07 TEXT, "
                + " S_DIST_08 TEXT, "
                + " S_DIST_09 TEXT, "
                + " S_DIST_10 TEXT, "
                + " S_DATA TEXT, "
                + " PRIMARY KEY (S_W_ID, S_I_ID) "
                + " );";

        session.execute(createWarehouseQuery);
        System.out.println(Table.getCreateTableSuccessMessage(Table.TABLE_WAREHOUSE));
        session.execute(createDistrictQuery);
        System.out.println(Table.getCreateTableSuccessMessage(Table.TABLE_DISTRICT));
        session.execute(createCustomerQuery);
        System.out.println(Table.getCreateTableSuccessMessage(Table.TABLE_CUSTOMER));
        session.execute(createOrderQuery);
        System.out.println(Table.getCreateTableSuccessMessage(Table.TABLE_ORDER));
        session.execute(createItemQuery);
        System.out.println(Table.getCreateTableSuccessMessage(Table.TABLE_ITEM));
        session.execute(createOrderLineQuery);
        System.out.println(Table.getCreateTableSuccessMessage(Table.TABLE_ORDERLINE));
        session.execute(createStockQuery);
        System.out.println(Table.getCreateTableSuccessMessage(Table.TABLE_STOCK));
    }

    private void createView() {
        String createCustomerBalancesViewCommand = "CREATE MATERIALIZED VIEW " + KEY_SPACE + "." + Table.VIEW_CUSTOMER_BALANCES + " AS "
                + " SELECT C_ID from " + KEY_SPACE + "." + Table.TABLE_CUSTOMER
                + " WHERE C_W_ID IS NOT NULL AND C_D_ID IS NOT NULL AND C_ID IS NOT NULL "
                + " AND C_BALANCE IS NOT NULL "
                + " PRIMARY KEY (C_BALANCE, C_W_ID, C_D_ID, C_ID)"
                + " WITH CLUSTERING ORDER BY (C_BALANCE DESC)";
        session.execute(createCustomerBalancesViewCommand);
        System.out.println("Successfully created materialized view : " + Table.VIEW_CUSTOMER_BALANCES);
    }

    private void loadData() {
        loadWarehouseData();
        loadDistrictData();
        loadCustomerData();
        loadOrderData();
        loadItemData();
        loadOrderLineData();
        loadStockData();
    }

    private void loadWarehouseData() {
        String insertWarehouseQuery = "INSERT INTO " + KEY_SPACE + "." + Table.TABLE_WAREHOUSE + " ("
                + " W_ID, W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP, "
                + " W_TAX, W_YTD ) "
                + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?); ";
        PreparedStatement insertWarehouseStatement = session.prepare(insertWarehouseQuery);
        String currLine;

        try {
            System.out.println(Table.getLoadingMessage(Table.TABLE_WAREHOUSE));
            FileReader fr = new FileReader(Table.getDataFileLocation(Table.FILE_WAREHOUSE));
            BufferedReader bf = new BufferedReader(fr);
            while ((currLine = bf.readLine()) != null) {
                String[] line = currLine.split(",");
                BoundStatement bs = insertWarehouseStatement.bind(
                        Integer.parseInt(line[0]), line[1], line[2],
                        line[3], line[4], line[5], line[6],
                        new BigDecimal(line[7]), new BigDecimal(line[8]));
                session.execute(bs);
            }
        } catch (IOException ioe) {
            //System.out.println("Working Directory = " +
            //        System.getProperty("user.dir"));
            System.out.println(Table.getLoadingErrorMessage(Table.TABLE_WAREHOUSE) + ioe.getMessage());
        }
    }

    private void loadDistrictData() {
        String insertDistrictQuery = "INSERT INTO " + KEY_SPACE + "." + Table.TABLE_DISTRICT + " ("
                + " D_W_ID, D_ID, D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, "
                + " D_TAX, D_YTD, D_NEXT_O_ID ) "
                + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?); ";
        PreparedStatement insertDistrictStatement = session.prepare(insertDistrictQuery);
        String currLine;

        try {
            System.out.println(Table.getLoadingMessage(Table.TABLE_DISTRICT));
            FileReader fr = new FileReader(Table.getDataFileLocation(Table.FILE_DISTRICT));
            BufferedReader bf = new BufferedReader(fr);
            while ((currLine = bf.readLine()) != null) {
                String[] line = currLine.split(",");
                BoundStatement bs = insertDistrictStatement.bind(
                        Integer.parseInt(line[0]), Integer.parseInt(line[1]),
                        line[2], line[3], line[4], line[5], line[6], line[7],
                        new BigDecimal(line[8]), new BigDecimal(line[9]),
                        Integer.parseInt(line[10]));
                session.execute(bs);
            }
        } catch (IOException ioe) {
            System.out.println(Table.getLoadingErrorMessage(Table.TABLE_DISTRICT) + ioe.getMessage());
        }
    }

    private void loadCustomerData() {
        String insertCustomerQuery = "INSERT INTO " + KEY_SPACE + "." + Table.TABLE_CUSTOMER + " ("
                + " C_W_ID, C_D_ID, C_ID, C_FIRST, C_MIDDLE, C_LAST, "
                + " C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, "
                + " C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, "
                + " C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DELIVERY_CNT, "
                + " C_DATA ) "
                + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?); ";
        PreparedStatement insertCustomerStatement = session.prepare(insertCustomerQuery);
        String currLine;

        try {
            int count = 0;
            System.out.println(Table.getLoadingMessage(Table.TABLE_CUSTOMER));
            FileReader fr = new FileReader(Table.getDataFileLocation(Table.FILE_CUSTOMER));
            BufferedReader bf = new BufferedReader(fr);
            while ((currLine = bf.readLine()) != null) {
                String[] line = currLine.split(",");
                BoundStatement bs = insertCustomerStatement.bind(
                        Integer.parseInt(line[0]), Integer.parseInt(line[1]), Integer.parseInt(line[2]),
                        line[3], line[4], line[5], line[6], line[7], line[8], line[9], line[10], line[11],
                        DF.parse(line[12]), line[13], new BigDecimal(line[14]), new BigDecimal(line[15]),
                        new BigDecimal(line[16]), Float.parseFloat(line[17]), Integer.parseInt(line[18]),
                        Integer.parseInt(line[19]), line[20]);
                session.execute(bs);
                count++;
                if (count % 3000 == 0) {
                    System.out.println(Table.getLoadingMessage(Table.TABLE_CUSTOMER) + " (" + Math.round((count/300000.0)*100) + "% done)");
                }
            }
        } catch (IOException ioe) {
            System.out.println(Table.getLoadingErrorMessage(Table.TABLE_CUSTOMER) + ioe.getMessage());
        } catch (ParseException pe) {
            System.out.println(Table.getLoadingErrorMessage(Table.TABLE_CUSTOMER) + pe.getMessage());
        }
    }

    private void loadOrderData() {
        String insertOrderQuery = "INSERT INTO " + KEY_SPACE + "." + Table.TABLE_ORDER + " ("
                + " O_W_ID, O_D_ID, O_ID, O_C_ID, O_CARRIER_ID, "
                + " O_OL_CNT, O_ALL_LOCAL, O_ENTRY_D ) "
                + " VALUES (?, ?, ?, ?, ?, ?, ?, ?); ";
        PreparedStatement insertOrderStatement = session.prepare(insertOrderQuery);
        String currLine;

        try {
            System.out.println(Table.getLoadingMessage(Table.TABLE_ORDER));
            FileReader fr = new FileReader(Table.getDataFileLocation(Table.FILE_ORDER));
            BufferedReader bf = new BufferedReader(fr);
            int count = 0;
            while ((currLine = bf.readLine()) != null) {
                String[] line = currLine.split(",");
                BoundStatement bs = null; // O_CARRIER_ID
                if (!line[4].equals("null")) {
                    bs = insertOrderStatement.bind(
                            Integer.parseInt(line[0]), Integer.parseInt(line[1]), Integer.parseInt(line[2]),
                            Integer.parseInt(line[3]), Integer.parseInt(line[4]),
                            new BigDecimal(line[5]), new BigDecimal(line[6]),
                            DF.parse(line[7]));
                } else {
                    bs = insertOrderStatement.bind(
                            Integer.parseInt(line[0]), Integer.parseInt(line[1]), Integer.parseInt(line[2]),
                            Integer.parseInt(line[3]), null,
                            new BigDecimal(line[5]), new BigDecimal(line[6]),
                            DF.parse(line[7]));
                }
                session.execute(bs);
                count++;
                if (count % 3000 == 0) {
                    System.out.println(Table.getLoadingMessage(Table.TABLE_ORDER) + " (" + Math.round((count/300000.0)*100) + "% done)");
                }
            }
        } catch (IOException ioe) {
            System.out.println(Table.getLoadingErrorMessage(Table.TABLE_ORDER) + ioe.getMessage());
        } catch (ParseException pe) {
            System.out.println(Table.getLoadingErrorMessage(Table.TABLE_ORDER) + pe.getMessage());
        }
    }

    private void loadItemData() {
        String insertItemQuery = "INSERT INTO " + KEY_SPACE + "." + Table.TABLE_ITEM + " ("
                + " I_ID, I_NAME, I_PRICE, I_IM_ID, I_DATA ) "
                + " VALUES (?, ?, ?, ?, ?); ";
        PreparedStatement insertItemStatement = session.prepare(insertItemQuery);
        String currLine;

        try {
            int count = 0;
            System.out.println(Table.getLoadingMessage(Table.TABLE_ITEM));
            FileReader fr = new FileReader(Table.getDataFileLocation(Table.FILE_ITEM));
            BufferedReader bf = new BufferedReader(fr);
            while ((currLine = bf.readLine()) != null) {
                String[] line = currLine.split(",");
                BoundStatement bs = insertItemStatement.bind(
                        Integer.parseInt(line[0]), line[1], new BigDecimal(line[2]),
                        Integer.parseInt(line[3]), line[4]);
                session.execute(bs);
                count++;
                if (count % 1000 == 0) {
                    System.out.println(Table.getLoadingMessage(Table.TABLE_ITEM) + " (" + Math.round((count/100000.0)*100) + "% done)");
                }
            }
        } catch (IOException ioe) {
            System.out.println(Table.getLoadingErrorMessage(Table.TABLE_ITEM) + ioe.getMessage());
        }
    }

    private void loadOrderLineData() {
        String insertOrderLineQuery = "INSERT INTO " + KEY_SPACE + "." + Table.TABLE_ORDERLINE + " ("
                + " OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER, OL_I_ID, "
                + " OL_DELIVERY_D, OL_AMOUNT, OL_SUPPLY_W_ID, OL_QUANTITY, OL_DIST_INFO ) "
                + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?); ";
        PreparedStatement insertOrderLineStatement = session.prepare(insertOrderLineQuery);
        String currLine;

        try {
            int count = 0;
            System.out.println(Table.getLoadingMessage(Table.TABLE_ORDERLINE));
            FileReader fr = new FileReader(Table.getDataFileLocation(Table.FILE_ORDERLINE));
            BufferedReader bf = new BufferedReader(fr);
            while ((currLine = bf.readLine()) != null) {
                String[] line = currLine.split(",");
                BoundStatement bs = null; // O_DELIVERY_D
                if (!line[5].equals("null")) {
                    bs = insertOrderLineStatement.bind(
                            Integer.parseInt(line[0]), Integer.parseInt(line[1]), Integer.parseInt(line[2]),
                            Integer.parseInt(line[3]), Integer.parseInt(line[4]), DF.parse(line[5]),
                            new BigDecimal(line[6]), Integer.parseInt(line[7]),
                            new BigDecimal(line[8]), line[9]);
                } else {
                    bs = insertOrderLineStatement.bind(
                            Integer.parseInt(line[0]), Integer.parseInt(line[1]), Integer.parseInt(line[2]),
                            Integer.parseInt(line[3]), Integer.parseInt(line[4]), null,
                            new BigDecimal(line[6]), Integer.parseInt(line[7]),
                            new BigDecimal(line[8]), line[9]);
                }
                session.execute(bs);
                count++;
                if (count % 3750 == 0) {
                    System.out.println(Table.getLoadingMessage(Table.TABLE_ORDERLINE) + " (" + Math.round((count/3748796.0)*1000) / 10.0 + "% done)");
                }
            }
        } catch (IOException ioe) {
            System.out.println(Table.getLoadingErrorMessage(Table.TABLE_ORDERLINE) + ioe.getMessage());
        } catch (ParseException pe) {
            System.out.println(Table.getLoadingErrorMessage(Table.TABLE_ORDERLINE) + pe.getMessage());
        }
    }

    private void loadStockData() {
        String insertStockQuery = "INSERT INTO " + KEY_SPACE + "." + Table.TABLE_STOCK + " ("
                + " S_W_ID, S_I_ID, S_QUANTITY, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, "
                + " S_DIST_01, S_DIST_02, S_DIST_03, S_DIST_04, S_DIST_05, "
                + " S_DIST_06, S_DIST_07, S_DIST_08, S_DIST_09, S_DIST_10, "
                + " S_DATA ) "
                + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?); ";
        PreparedStatement insertStockStatement = session.prepare(insertStockQuery);
        String currLine;

        try {
            int count = 0;
            System.out.println(Table.getLoadingMessage(Table.TABLE_STOCK));
            FileReader fr = new FileReader(Table.getDataFileLocation(Table.FILE_STOCK));
            BufferedReader bf = new BufferedReader(fr);
            while ((currLine = bf.readLine()) != null) {
                String[] line = currLine.split(",");
                BoundStatement bs = insertStockStatement.bind(
                        Integer.parseInt(line[0]), Integer.parseInt(line[1]), new BigDecimal(line[2]),
                        new BigDecimal(line[3]), Integer.parseInt(line[4]),
                        Integer.parseInt(line[5]), line[6], line[7], line[8], line[9], line[10],
                        line[11], line[12], line[13], line[14], line[15], line[16]);
                session.execute(bs);
                count++;
                if (count % 1000 == 0) {
                    System.out.println(Table.getLoadingMessage(Table.TABLE_STOCK) + " (" + Math.round((count/1000000.0)*1000) / 10.0 + "% done)");
                }
            }
        } catch (IOException ioe) {
            System.out.println(Table.getLoadingErrorMessage(Table.TABLE_STOCK) + ioe.getMessage());
        }
    }
}
