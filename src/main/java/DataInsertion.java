import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

public class DataInsertion {
    static final String[] CONTACT_POINTS = null;
    static final String KEY_SPACE = null;

    private Session session;

    public static void main(String[] args) {
        DataInsertion di = new DataInsertion();
        di.run();
    }

    public void run() {
        Cluster cluster = Cluster.builder().addContactPoints(CONTACT_POINTS).build();
        session = cluster.connect();
    }
}
