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

    public static final String VIEW_CUSTOMER_BALANCES = "customer_balances";
    public static final String VIEW_ORDERED_ITEMS = "ordered_items";
    public static final String VIEW_ORDER_PARTITIONED_BY_CUSTOMER = "order_partitioned_by_customer";
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

    public static final String PERFORMANCE_OUTPUT_PATH = "performance_output.txt";

    public static final String KEY_SPACE = "wholesale_supplier";

    public static final String[] IP_ADDRESSES = {"192.168.48.239","192.168.48.240","192.168.48.241",
            "192.168.48.242","192.168.48.243"};

    public static String getCreateTableSuccessMessage(String tableName) {
        return "Successfully created table : " + tableName;
    }

    public static String getLoadingMessage(String tableName) {
        return "Loading data for table : " + tableName;
    }

    public static String getDataFileLocation(String fileNameWithoutExtension) {
        return "data/" + fileNameWithoutExtension + ".csv";
    }

    public static String getTransactionFileLocation(int fileNameWithoutExtension) {
        return "xact/" + fileNameWithoutExtension + ".txt";
    }

    public static String getLoadingErrorMessage(String tableName) {
        return "Error loading " + tableName + " data : ";
    }
}
