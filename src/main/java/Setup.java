package com.cassandra.app;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

public class Setup {
    static final String[] CONTACT_POINTS = {"localhost"};
    static final String KEY_SPACE = "wholesale_supplier";
    static final int REPLICATION_FACTOR = 3;

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
        loadData();

        System.out.println("Setup is done!");

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
        String createWarehouseQuery = "CREATE TABLE " + KEY_SPACE + "." + Table.WAREHOUSE + " ("
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

        String createDistrictQuery = "CREATE TABLE " + KEY_SPACE + "." + Table.DISTRICT + " ("
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

        String createCustomerQuery = "CREATE TABLE " + KEY_SPACE + "." + Table.CUSTOMER + " ("
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

        String createOrderQuery = "CREATE TABLE " + KEY_SPACE + "." + Table.ORDER + " ("
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

        String createItemQuery = "CREATE TABLE " + KEY_SPACE + "." + Table.ITEM + " ("
                + " I_ID INT, "
                + " I_NAME TEXT, "
                + " I_PRICE DECIMAL, "
                + " I_IM_ID INT, "
                + " I_DATA TEXT, "
                + " PRIMARY KEY (I_ID) "
                + ");";

        String createOrderLineQuery = "CREATE TABLE " + KEY_SPACE + "." + Table.ORDER_LINE + " ("
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

        String createStockQuery = "CREATE TABLE " + KEY_SPACE + "." + Table.STOCK + " ("
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
        System.out.println("Successfully created table : " + Table.WAREHOUSE);
        session.execute(createDistrictQuery);
        System.out.println("Successfully created table : " + Table.DISTRICT);
        session.execute(createCustomerQuery);
        System.out.println("Successfully created table : " + Table.CUSTOMER);
        session.execute(createOrderQuery);
        System.out.println("Successfully created table : " + Table.ORDER);
        session.execute(createItemQuery);
        System.out.println("Successfully created table : " + Table.ITEM);
        session.execute(createOrderLineQuery);
        System.out.println("Successfully created table : " + Table.ORDER_LINE);
        session.execute(createStockQuery);
        System.out.println("Successfully created table : " + Table.STOCK);
    }

    private void loadData() {
        loadWarehouseData();
    }

    private void loadWarehouseData() {
        String insertWarehouseQuery = "INSERT INTO " + KEY_SPACE + ".warehouse ("
                + " W_ID, W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP, "
                + " W_TAX, W_YTD ) "
                + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?); ";
        PreparedStatement insertWarehouseStatement = session.prepare(insertWarehouseQuery);
        String currLine;

        try {
            System.out.println("Loading data for table : warehouse");
            FileReader fr = new FileReader("data/warehouse.csv");
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
            System.out.println("Working Directory = " +
                    System.getProperty("user.dir"));
            System.out.println("Error loading warehouse data : " + ioe.getMessage());
        }
    }
}
