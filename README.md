# supplier_cassandra

### Introduction

This system uses cassandra and five server nodes to create a distributed database that supports the supplier-warehouse purpose transactions.

### Prerequisite
- [Cassandra 3.11](http://www.apache.org/dyn/closer.lua/cassandra/3.11.0/apache-cassandra-3.11.0-bin.tar.gz) and above
- [Java 8](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html) and above
- [Maven 3.5.4](https://maven.apache.org/download.cgi) and above
- [Datastax 3.3.9](https://academy.datastax.com/quick-downloads) and above

For Windows users, ensure that paths to Java 8 is correctly added to `System Properties` > `Advanced` > `Environment Variables` > `PATH`.

Place the cassandra folder into all five server nodes.

The other prerequisites should be placed in the root folder of the supplier-cassandra program

### Configuration for five nodes
```
vim cassandra.yaml
```
Edit the settings in 'cassandra.yaml' file for each servers:

1) seeds_ address: Add the IP addresses of the five nodes.
    
2) listen_address: Add in the IP address of the current node in use.

Save the file and restart/run the cassandra server. Repeat this for all five servers

### Import Data To Project

1. Create a folder `data` and `xact`, if not available, in the same file level as your program `src` folder.
2. Insert the data files (in .csv format) into the `data` folder.
3. Insert the xact files (in .txt format) into the `xact` folder.
4. Copy the `initSetup.txt` file into the same file level as `src`. You may edit the values in the file to accomodate your machine. (`K` stands for keyspace used by Cassandra, while `I` stands for IP addresses which are the contact points)
5. In project root folder, compile the project via `mvn clean dependency:copy-dependencies package`. If the `mvn` command is not available, type the command: `export PATH=<file location of Apache Maven>:$PATH` (e.g. `export PATH=/home/stuproj/cs4224e/apache-maven-3.5.0/bin:$PATH`), then run the `mvn` command again.
6. Run `java -cp target/*:target/dependency/*:. Setup` once to create key space and schemas used in this project.

### Performance Measurement

After setting up, simply run the following command to run the performance management:

`java -cp target/*:target/dependency/*:. Main [numClients] [consistencyType]`

* `numClients` is the number of clients
* `consistencyType` is the consistency level used in Cassandra (`ONE` or `QUORUM`).

Example:

`java -cp target/*:target/dependency/*:. Main 10 ONE`

### Transaction Files Format

The files in the `xact` folder should be named in this format:

`<number>.txt`

where the `number` is the client number. For example, if you have three clients, there should be `1.txt`, `2.txt` and `3.txt` in the folder.

The files in the `xact` folder should follow these format to represent the transactions:

**New Transaction: `N`**

Format

    N, W_ID, D_ID, C_ID, NUM_ITEMS
	ITEM_NUMBER[i], SUPPLIER_WAREHOUSE[i], QUANTITY[i]
> Process a new transaction from a custormer.
> 
> Note that 1 <= NUM_ITEMS <= 20, i ∈ [1,NUM ITEMS]

Examples:

    N,347,7,7,3
    14,10,68
    283,7,40
    312,12,10


**Payment Transaction: `P`**

Format

    P, W_ID, D_ID, C_ID, PAYMENT

> Process a payment made by a customer.

**Delivery Transaction: `D`**

Format

    D, W_ID, CARRIER_ID

> Process the delivery of the oldest yet-to-be-delivered order for each of the 10
districts in a specified warehouse.   

**Order-Status Transaction: `O`**

Format

    O, W_ID, D_ID, C_ID

> Query the status of the last order of a customer

**Stock-level Transaction: `S`**

Format

    S, W_ID, D_ID, T, L

> Examine the items from the last L orders at a specified warehouse district and reports the number of those items that have a stock level below a specified threshold.

**Popular-Item Transaction: `I`**

Format

    I, W_ID, D_ID, L

> Find the most popular item(s) in each of the last L orders at a specified warehouse district. 
> Given two items X and Y in the same order O, X is defined to be more popular than Y in O if the quantity ordered for X in O is greater than the quantity ordered for Y in O.

**Top-Balance Transaction: `T`**

Format
     
     T     
> Find the top 10 customers ranked in descending order of their outstanding balance payments.

**Related Customer Transaction: `R`**

Format

    R, C_W_ID, C_D_ID, C_ID

> Finds all the customers that are related to a specific customer. Given a customer C. another customer C' is defined to be related to C if all the following conditions hold. C and C' are associated with different warehouses. C and C' each has placed some order, O and O', respectively, where both O and O' contain at least two items in common.