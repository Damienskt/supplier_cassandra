import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

import transaction.TopBalanceTransaction;

public class Main {
    static final String[] CONTACT_POINTS = {"localhost"};
    private static Session session;
    public static void main(String[] args) {
        Cluster cluster = Cluster.builder().addContactPoints(CONTACT_POINTS).build();
        session = cluster.connect();
        TopBalanceTransaction test = new TopBalanceTransaction(session);
        test.calTopBalance();
    }
}
