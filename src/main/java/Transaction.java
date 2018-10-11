import java.math.BigDecimal;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.Session;

import transaction.*;

/**
 * Implementation of the eight transaction types.
 * 1) New Order Transaction
 * 2) Payment Transaction
 * 3) Delivery Transaction
 * 4) Order-Status Transaction
 * 5) Stock-level Transaction
 * 6) Popular-Item Transaction
 * 7) Top-Balance Transaction
 * 8) Related-Customer Transaction
 */
public class Transaction {
    public static final String[] CONTACT_POINTS = Setup.CONTACT_POINTS;
    public static final String KEY_SPACE = Setup.KEY_SPACE;

    private Session session;
    private NewOrderTransaction newOrderTransaction;
    private PaymentTransaction paymentTransaction;
    private DeliveryTransaction deliveryTransaction;
    private OrderStatusTransaction orderStatusTransaction;
    private StockLevelTransaction stockLevelTransaction;
    private PopularItemTransaction popularItemTransaction;
    private TopBalanceTransaction topBalanceTransaction;
    private RelatedCustomerTransaction relatedCustomerTransaction;

    public Transaction(int index, String consistencyLevel) {
        QueryOptions queryOptions;
        if (consistencyLevel.equalsIgnoreCase("ONE")) {
            queryOptions = new QueryOptions().setConsistencyLevel(ConsistencyLevel.ONE);
        } else {
            queryOptions = new QueryOptions().setConsistencyLevel(ConsistencyLevel.QUORUM);
        }

        Cluster cluster = Cluster.builder()
                .addContactPoint(CONTACT_POINTS[index % 5])
                .withQueryOptions(queryOptions)
                .build();
        session = cluster.connect();

        newOrderTransaction = new NewOrderTransaction(session);
        paymentTransaction = new PaymentTransaction(session);
        deliveryTransaction = new DeliveryTransaction(session);
        orderStatusTransaction = new OrderStatusTransaction(session);
        stockLevelTransaction = new StockLevelTransaction(session);
        popularItemTransaction = new PopularItemTransaction(session);
        topBalanceTransaction = new TopBalanceTransaction(session);
        relatedCustomerTransaction = new RelatedCustomerTransaction(session);
    }

    public void processNewOrder(int wId, int dId, int cId, int numItems,
            int[] itemNum, int[] supplierWarehouse, int[] qty) {
        newOrderTransaction.processOrder(wId, dId, cId, numItems, itemNum, supplierWarehouse, qty);
    }

    public void processPayment(int wId, int dId, int cId, float payment) {
        paymentTransaction.processPaymentTransaction(wId, dId, cId, payment);
    }

    public void processDelivery(int wId, int carrierId) {
        deliveryTransaction.processDeliveryTransaction(wId, carrierId);
    }

    public void processOrderStatus(int wId, int dId, int cId) {
        orderStatusTransaction.processOrderStatus(wId, dId, cId);
    }

    public void processStockLevel(int wId, int dId, BigDecimal T, int L) {
        stockLevelTransaction.processStockLevelTransaction(wId, dId, T, L);
    }

    public void processPopularItem(int wId, int dId, int L) {
        popularItemTransaction.popularItem(wId, dId, L);
    }

    public void processTopBalance() {
        topBalanceTransaction.calTopBalance();
    }

    public void processRelatedCustomer(int wId, int dId, int cId) {
        relatedCustomerTransaction.relatedCustomer(wId, dId, cId);
    }
}
