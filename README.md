CBDE
====

Practica CBDE MJ &amp; Clara
  
  

## Oracle

To run the project, you will have to provide the following parameters to successfully connect to the remote Oracle DB:

	-username username -password password
	
### Schema

The schema must be created previous to executing the Java project. There are two schemas, the normalized and tuned versions which can be found in the scripts subfolder. The are named "*Material-Lab-TPC-H.sql*" and "*OptimizedOracleSchema.sql*" respectively.

The script containing other tuning possibilities tried can also be found in the scripts subfolder and is named "*Practica CBDE.sql*", as well as other scripts which modify the schema for Primary Key, Cluster and Hash index creation.

### Insertions

Insertions are done at random, with a fixed seed to ensure that all executions generate the same data set. This distribution will be conserved for the other databases.

Values for table population are taken at random following the restrictions provided. To ensure that certain values exist to be queried, the attributes in the queries selected by value ensure a certain proportion of a fixed value. For example, at least 1 of every 20 LineItems has the attribute L_ShipDate set to April 30th, 2013. We have also included a few attributes with null values for 1 every 20 rows. Dates are uniformly distributed between 5000 days before and after April 30th, 2013.




### Resources used

* Mkyong.com, *Connect To Oracle DB Via JDBC Driver*, http://www.mkyong.com/jdbc/connect-to-oracle-db-via-jdbc-driver-java/

* Mkyong.com, *JDBC Tutorials*, http://www.mkyong.com/tutorials/jdbc-tutorials/




## Mongo

###Schema
The schema for the Mongo database is created during the Java execution. Therefore, it is unnecessary to create it previously. Data existing in the database prior to execution will be dropped before inserting any new data.
To run the project, ensure you have an instance of MongoDB running on localhost port 27017.

### Resources used

* Mongodb.org, *Getting started with Java Driver*, http://docs.mongodb.org/ecosystem/tutorial/getting-started-with-java-driver/#getting-started-with-java-driver