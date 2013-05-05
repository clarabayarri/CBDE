import java.net.UnknownHostException;

import com.mongodb.DB;
import com.mongodb.MongoClient;


public class MongoNormalized {

	private static DB database;
	
	private static DataInserter dataInserter = new DataInserter();
	
	public static void main(String[] argv) throws Exception {
		System.out.println("--------- GOOD DAY MONGO-NORMALIZED!!! ----------\n");
		
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
		
		// query
		
		dataInserter.secondInsert(database);
		
		// query
		
	}
	
	public static DB getMongoDB() throws UnknownHostException {
		MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
		DB db = mongoClient.getDB( "cbde-normalized" );
		return db;
	}
}
