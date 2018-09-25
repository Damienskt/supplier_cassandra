package constant;

/**
 * Global constants for use related to tables
 */
public class Table {

    /**
     * Table names used in the database
     */
    public static final String TABLE_WAREHOUSE = "warehouse";
    public static final String TABLE_DISTRICT = "district";
    public static final String TABLE_CUSTOMER = "customer";
    public static final String TABLE_ORDER = "orders";
    public static final String TABLE_ITEM = "item";
    public static final String TABLE_ORDERLINE = "order_line";
    public static final String TABLE_STOCK = "stock";

    /**
     *  File names of the data
     */
    public static final String FILE_WAREHOUSE = "warehouse";
    public static final String FILE_DISTRICT = "district";
    public static final String FILE_CUSTOMER = "customer";
    public static final String FILE_ORDER = "order";
    public static final String FILE_ITEM = "item";
    public static final String FILE_ORDERLINE = "order-line";
    public static final String FILE_STOCK = "stock";

    public static String getCreateTableSuccessMessage(String tableName) {
        return "Successfully created table : " + tableName;
    }

    public static String getLoadingMessage(String tableName) {
        return "Loading data for table : " + tableName;
    }

    public static String getDataFileLocation(String fileNameWithoutExtension) {
        return "data/" + fileNameWithoutExtension + ".csv";
    }

    public static String getLoadingErrorMessage(String tableName) {
        return "Error loading " + tableName + " data : ";
    }
}
