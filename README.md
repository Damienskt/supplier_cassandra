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

Download the csv data files from..

Store the csv data files in the same file level as your program src folder by creating a new `data` folder and moving all the csv files into this folder.

After importing the csv data files, run setup.java for the program to store this data into the distributed database. Be sure to have all five servers running when executing setup.java.

### Run Client

The `gradlew` script assumes by default that Cassandra runs on IP address `192.168.48.229`, the address of experiment node of team CS4224C. 

To change default IP address, change `cassandra.ip` in file `client/project.properties`. 

If more than one node is involved, the IP address could be from any one of the nodes.

To change consistency level of Cassandra, change `query.consistency.level` in file `client/project.properties` to your required Cassandra consistency level. This change applies to all queries.

Run command `gradlew client:run -q` at project root.

Alternatively, open Java IDE installed and import the project as `Gradle Project`. Make sure `Auto Import` is enabled.

`Build Project` and make sure there are no missing dependencies. 

Find and Run `Client.java` at `client/src/main/java/client/cs4224c/`. 


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