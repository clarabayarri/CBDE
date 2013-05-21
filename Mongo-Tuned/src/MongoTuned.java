import java.net.UnknownHostException;

import com.mongodb.DB;
import com.mongodb.MongoClient;


public class MongoTuned {

	private static DB database;
	
	private static DataInserter dataInserter = new DataInserter();
	
	private static QuerySet querySet = new QuerySet(); 
	
	public static void main(String[] argv) throws Exception {
		System.out.println("--------- GOOD DAY MONGO-TUNED!!! ----------\n");
		
		database = getMongoDB();
		if (database != null) {
			System.out.println("Connection succeeded\n");
		} else {
			System.out.println("Connection failed! Please check.");
			return;
		}
		
		// Drop previous data
		database.dropDatabase();
		
		dataInserter.initialInsert(database);
		
		querySet.executeQueries(database);
		
		dataInserter.secondInsert(database);
		
		querySet.executeQueries(database);
		
	}
	
	public static DB getMongoDB() throws UnknownHostException {
		MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
		DB db = mongoClient.getDB( "cbde-tuned" );
		return db;
	}
}
