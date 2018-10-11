import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.math.BigDecimal;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

import constant.Table;
import transaction.DeliveryTransaction;
import transaction.NewOrderTransaction;
import transaction.OrderStatusTransaction;
import transaction.PaymentTransaction;
import transaction.PopularItemTransaction;
import transaction.RelatedCustomerTransaction;
import transaction.StockLevelTransaction;
import transaction.TopBalanceTransaction;

public class Main {
    private int numberOfClients;
    private String consistencyLevel;

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
    }

    public void executeQueries() {
        // Initialize connector
        //Cluster cluster = Cluster.builder().addContactPoints("localhost").build();
        //Session session = cluster.connect();

        // Initialize all transactions
        /*NewOrderTransaction nOT = new NewOrderTransaction(session);
        OrderStatusTransaction oST = new OrderStatusTransaction(session);
        DeliveryTransaction dT = new DeliveryTransaction(session);
        StockLevelTransaction sLT = new StockLevelTransaction(session);
        PaymentTransaction pT = new PaymentTransaction(session);
        PopularItemTransaction pIT = new PopularItemTransaction(session);
        TopBalanceTransaction tBT = new TopBalanceTransaction(session);
        RelatedCustomerTransaction rCT = new RelatedCustomerTransaction(session);*/
        //String pathTemplate = "../data/%d.txt";

        long[] transactionsTiming = new long[8];
        int[] transactionsExecutedCount = new int[8];
        long startTime;
        for (int index = 0; index < numberOfClients; index++) {
            File file = new File(Table.getTransactionFileLocation(index));
            Transaction transaction = new Transaction(index, this.consistencyLevel);
            try {
                BufferedReader reader = new BufferedReader(new FileReader(file));
                String input = reader.readLine();

                while (input != null && input.length() > 0) {
                    String[] arguments = input.split(",");

                    if (input.charAt(0) == 'N') {
                        int cId = Integer.parseInt(arguments[1]);
                        int wId = Integer.parseInt(arguments[2]);
                        int dId = Integer.parseInt(arguments[3]);
                        int numItems = Integer.parseInt(arguments[4]);
                        int[] itemNumbers = new int[numItems];
                        int[] supplierWarehouse = new int[numItems];
                        int[] quantity = new int[numItems];

                        String newInputLine;
                        String[] newArguments;
                        for (int j = 0; j < numItems; j++) {
                            newInputLine = reader.readLine();
                            newArguments = newInputLine.split(",");

                            itemNumbers[j] = Integer.parseInt(newArguments[0]);
                            supplierWarehouse[j] = Integer.parseInt(newArguments[1]);
                            quantity[j] = Integer.parseInt(newArguments[2]);
                        }

                        try {
                            startTime = System.nanoTime();
                            transaction.processNewOrder(wId, dId, cId, numItems, itemNumbers, supplierWarehouse, quantity);
                            long endTime = System.nanoTime() - startTime;
                            updateTransactionDetail(0, endTime, transactionsTiming, transactionsExecutedCount);
                        } catch (Exception e) {
                            System.err.println(e.getMessage());
                        }
                    } else if (input.charAt(0) == 'P') {
                        int wId = Integer.parseInt(arguments[1]);
                        int dId = Integer.parseInt(arguments[2]);
                        int cId = Integer.parseInt(arguments[3]);
                        float payment = Float.parseFloat(arguments[4]);

                        try {
                            startTime = System.nanoTime();
                            transaction.processPayment(wId, dId, cId, payment);
                            long endTime = System.nanoTime() - startTime;
                            updateTransactionDetail(1, endTime, transactionsTiming, transactionsExecutedCount);
                        } catch (Exception e) {
                            System.err.println(e.getMessage());
                        }
                    } else if (input.charAt(0) == 'D') {
                        int wId = Integer.parseInt(arguments[1]);
                        int carrierId = Integer.parseInt(arguments[2]);

                        try {
                            startTime = System.nanoTime();
                            transaction.processDelivery(wId, carrierId);
                            long endTime = System.nanoTime() - startTime;
                            updateTransactionDetail(2, endTime, transactionsTiming, transactionsExecutedCount);
                        } catch (Exception e) {
                            System.err.println(e.getMessage());
                        }
                    } else if (input.charAt(0) == 'O') { // Order Status
                        int wId = Integer.parseInt(arguments[1]);
                        int dId = Integer.parseInt(arguments[2]);
                        int cId = Integer.parseInt(arguments[3]);

                        try {
                            startTime = System.nanoTime();
                            transaction.processOrderStatus(wId, dId, cId);
                            long endTime = System.nanoTime() - startTime;
                            updateTransactionDetail(3, endTime, transactionsTiming, transactionsExecutedCount);
                        } catch (Exception e) {
                            System.err.println(e.getMessage());
                        }
                    } else if (input.charAt(0) == 'S') {
                        int wId = Integer.parseInt(arguments[1]);
                        int dId = Integer.parseInt(arguments[2]);
                        int T = Integer.parseInt(arguments[3]);
                        int L = Integer.parseInt(arguments[4]);

                        try {
                            startTime = System.nanoTime();
                            transaction.processStockLevel(wId, dId, new BigDecimal(T), L);
                            long endTime = System.nanoTime() - startTime;
                            updateTransactionDetail(4, endTime, transactionsTiming, transactionsExecutedCount);
                        } catch (Exception e) {
                            System.err.println(e.getMessage());
                        }
                    } else if (input.charAt(0) == 'I') {
                        int wId = Integer.parseInt(arguments[1]);
                        int dId = Integer.parseInt(arguments[2]);
                        int L = Integer.parseInt(arguments[3]);

                        try {
                            startTime = System.nanoTime();
                            transaction.processPopularItem(wId, dId, L);
                            long endTime = System.nanoTime() - startTime;
                            updateTransactionDetail(5, endTime, transactionsTiming, transactionsExecutedCount);
                        } catch (Exception e) {
                            System.err.println(e.getMessage());
                        }
                    } else if (input.charAt(0) == 'T') {
                        try {
                            startTime = System.nanoTime();
                            transaction.processTopBalance();
                            long endTime = System.nanoTime() - startTime;
                            updateTransactionDetail(6, endTime, transactionsTiming, transactionsExecutedCount);
                        } catch (Exception e) {
                            System.err.println(e.getMessage());
                        }
                    } else if (input.charAt(0) == 'R') {
                        int wId = Integer.parseInt(arguments[1]);
                        int dId = Integer.parseInt(arguments[2]);
                        int cId = Integer.parseInt(arguments[3]);

                        try {
                            startTime = System.nanoTime();
                            transaction.processRelatedCustomer(wId, dId, cId);
                            long endTime = System.nanoTime() - startTime;
                            updateTransactionDetail(7, endTime, transactionsTiming, transactionsExecutedCount);
                        } catch (Exception e) {
                            System.err.println(e.getMessage());
                        }
                    } else {
                        System.err.println("\n\nOops, the application encountered an error in reading file.\n\n");
                    }
                    System.out.println();
                    input = reader.readLine();
                }

                reader.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        //session.close();

        float totalTime = (float)0.0;
        float duration;
        float throughput;
        int totalTransactionExecuted = 0;
        for (int i = 0; i < 8; i++) {
            if (transactionsExecutedCount[i] > 0) {
                duration = (float) transactionsTiming[i] / 1000000000;
                throughput = (float) (transactionsExecutedCount[i]) / duration;
                totalTime = totalTime + duration;
                totalTransactionExecuted = totalTransactionExecuted + transactionsExecutedCount[i];

                printIndividualTransactionResult(i, transactionsExecutedCount, duration, throughput);
            }
        }

        throughput = (float)totalTransactionExecuted / totalTime;
        printOverallTransactionResult(totalTransactionExecuted, totalTime, throughput);
    }

    private void updateTransactionDetail(int index, long endTime, long[] transactionsTiming,
                                         int[] transactionsExecutedCount) {
        transactionsTiming[index] = transactionsTiming[index] + endTime;
        transactionsExecutedCount[index] = transactionsExecutedCount[index] + 1;
    }

    private void printIndividualTransactionResult(int i, int[] transactionsExecutedCount, float duration,
                                                  float throughput) {
        System.out.printf("Transaction %d total transactions: %d \n", i, transactionsExecutedCount[i]);
        System.out.printf("Transaction %d time used: %f s \n", i, duration);
        System.out.printf("Transaction %d's Throughput: %f \n", i, throughput);
    }

    private void printOverallTransactionResult(int totalTransactionExecuted, float totalTime,
                                               float throughput) {
        System.out.printf("Total Transactions executed: %d \n", totalTransactionExecuted);
        System.out.printf("Total Time used: %fs \n", totalTime);
        System.out.printf("Aggregated Throughput: %f \n", throughput);
    }
}
