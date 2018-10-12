import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import constant.Table;

public class Main {
    private int numberOfClients;
    private String consistencyLevel;
    static String[] CONTACT_POINTS = Table.IP_ADDRESSES;
    static String KEY_SPACE = Table.KEY_SPACE;

    public static void main( String[] args ) {
        int numberOfClients;
        String consistencyLevel;
        if (args == null || args.length <= 0) {
            numberOfClients = 10;
            consistencyLevel = "ONE";
        } else {
            numberOfClients = (Integer.parseInt(args[0]));
            consistencyLevel = args[1];
        }

        System.out.println("Number of clients: " + numberOfClients
                + ", Consistency Level: " + consistencyLevel);

        Main app = new Main(numberOfClients, consistencyLevel);
        app.executeQueries();
    }

    public Main(int numberOfClients, String consistencyLevel) {
        this.numberOfClients = numberOfClients;
        this.consistencyLevel = consistencyLevel;
        readInitFile();
    }

    public void executeQueries() {
        ExecutorService executorService = Executors.newFixedThreadPool(Math.max(1, numberOfClients));
        List<Future<ClientStatistics>> results = new ArrayList();

        for (int index = 0; index < numberOfClients; index++) {
            Future<ClientStatistics> future = executorService.submit(new ClientThread(index, consistencyLevel,
                    CONTACT_POINTS, KEY_SPACE));
            results.add(future);
        }

        Map<Integer, ClientStatistics> statisticsMap = new HashMap();
        for (Future<ClientStatistics> future : results) {
            try {
                ClientStatistics statistics = future.get();
                statisticsMap.put(statistics.getIndex(), statistics);
            } catch (InterruptedException e) {
                e.printStackTrace();
                return;
            } catch (ExecutionException e) {
                e.printStackTrace();
                return;
            }
        }

        outputPerformanceResults(statisticsMap);
        System.out.println("\nAll " + numberOfClients + " clients have completed their transactions.");
    }

    private void readInitFile() {
        File file = new File(Table.FILE_IP_ADDRESSES);
        try {
            BufferedReader reader = new BufferedReader(new FileReader(file));
            String input = reader.readLine();

            while (input != null && input.length() > 0) {
                String[] arguments = input.split(",");

                if (input.charAt(0) == 'K') {
                    KEY_SPACE = arguments[1];
                } else if (input.charAt(0) == 'I') {
                    String[] ipAddresses = new String[arguments.length - 1];
                    for (int i = 0; i < ipAddresses.length; i++) {
                        ipAddresses[i] = arguments[i+1];
                    }
                    CONTACT_POINTS = ipAddresses;
                }
                input = reader.readLine();
            }
            reader.close();
            System.out.println("Key space used: " + KEY_SPACE);
            System.out.println("IP addresses:");
            for (int i = 0; i < CONTACT_POINTS.length; i++) {
                if (i != 0) {
                    System.out.print(",");
                }
                System.out.print(CONTACT_POINTS[i]);
            }
            System.out.println();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void outputPerformanceResults(Map<Integer, ClientStatistics> statisticsMap) {
        int numClients = statisticsMap.size();
        String outputPath = Table.PERFORMANCE_OUTPUT_PATH;

        try {
            PrintWriter out = new PrintWriter(outputPath);
            ClientStatistics min = statisticsMap.get(0);
            ClientStatistics max = statisticsMap.get(0);
            double totalThroughput = 0;
            for (ClientStatistics current : statisticsMap.values()) {
                double currentThroughput = current.getThroughput();
                totalThroughput += currentThroughput;
                if (currentThroughput < min.getThroughput()) {
                    min = current;
                } else if (currentThroughput > max.getThroughput()) {
                    max = current;
                }
            }

            // Output statistics of each client:
            for (int i = 0; i < numClients; i++) {
                ClientStatistics stats = statisticsMap.get(i);
                int index = i + 1;
                long totalTransactionCount = stats.getTotalTransactionCount();
                double totalExecutionTime = (double) stats.getTotalExecutionTime() / 1000000000;
                double throughput = stats.getThroughput();

                long[] transactionCountStats = stats.getAllTransactionCount();
                long[] executionTimeStats = stats.getAllExecutionTime();

                long newOrderTransactionCount = transactionCountStats[0];
                long paymentTransactionCount = transactionCountStats[1];
                long deliveryTransactionCount = transactionCountStats[2];
                long orderStatusTransactionCount = transactionCountStats[3];
                long stockLevelTransactionCount = transactionCountStats[4];
                long popularItemTransactionCount = transactionCountStats[5];
                long topBalanceTransactionCount = transactionCountStats[6];
                long relatedCustomerTransactionCount = transactionCountStats[7];

                double newOrderExecutionTime = (double) executionTimeStats[0] / 1000000000;
                double paymentExecutionTime = (double) executionTimeStats[1] / 1000000000;
                double deliveryExecutionTime = (double) executionTimeStats[2] / 1000000000;
                double orderStatusExecutionTime = (double) executionTimeStats[3] / 1000000000;
                double stockLevelExecutionTime = (double) executionTimeStats[4] / 1000000000;
                double popularItemExecutionTime = (double) executionTimeStats[5] / 1000000000;
                double topBalanceExecutionTime = (double) executionTimeStats[6] / 1000000000;
                double relatedCustomerExecutionTime = (double) executionTimeStats[7] / 1000000000;

                out.println("Performance measure for client with index: " + index);
                out.println("Total Number of Executed Transactions: " + totalTransactionCount);
                out.println("Total Execution Time: " + totalExecutionTime);
                out.println("Transaction throughput: " + throughput);
                out.println("-------------------------------------");
                out.println("New Order: " + newOrderTransactionCount + " " + newOrderExecutionTime);
                out.println("Payment: " + paymentTransactionCount + " " + paymentExecutionTime);
                out.println("Delivery Status: " + deliveryTransactionCount + " " + deliveryExecutionTime);
                out.println("Order Status: " + orderStatusTransactionCount + " " + orderStatusExecutionTime);
                out.println("Stock Level: " + stockLevelTransactionCount + " " + stockLevelExecutionTime);
                out.println("Popular Item: " + popularItemTransactionCount + " " + popularItemExecutionTime);
                out.println("Top Balance: " + topBalanceTransactionCount + " " + topBalanceExecutionTime);
                out.println("Related Customer: " + relatedCustomerTransactionCount + " " + relatedCustomerExecutionTime);
                out.println("=========================================");
                out.println();
            }

            out.println("== End of Performance Measure for each Client ==");
            out.println();

            // Output minimum, maximum and average throughput
            out.println("Minimum transaction throughput is the client with index " + (min.getIndex() + 1)
                + " with throughput: " + min.getThroughput());
            out.println("Minimum transaction throughput is the client with index " + (min.getIndex() + 1)
                    + " with throughput: " + min.getThroughput());
            out.println("Average transaction throughput is: " + (totalThroughput / numClients));

            out.close();
        } catch (FileNotFoundException e) {
            System.out.println("Error in outputting performance results.");
            e.printStackTrace();
        }
    }
}
