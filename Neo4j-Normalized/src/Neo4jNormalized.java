import java.io.File;
import java.io.IOException;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.impl.util.FileUtils;



public class Neo4jNormalized {
	
	private static GraphDatabaseService graphDB;
	private static final String DB_PATH = "../neo4j-normalized-db";
	private static DataInserter inserter = new DataInserter();
	private static QuerySet querySet = new QuerySet(); 

    public static void main( final String[] args )
    {
//    	inserter.removeData();
//    	inserter.shutDown();
    	
    	createDb();
    	
    	inserter.initialInsert( graphDB );
    	
    	querySet.executeQueries( graphDB );
    	
    	inserter.secondInsert( graphDB );
    	
    	querySet.executeQueries( graphDB );
    }
    
    
	static void createDb() {
		clearDb();
		graphDB = new GraphDatabaseFactory().newEmbeddedDatabase( DB_PATH );
		registerShutdownHook( graphDB );
	}

	private static void clearDb() {
		try {
			FileUtils.deleteRecursively( new File( DB_PATH ) );
		}
		catch ( IOException e ) {
			throw new RuntimeException( e );
		}
	}

	void shutDown() {
		System.out.println();
		System.out.println( "Shutting down database ..." );
		// START SNIPPET: shutdownServer
		graphDB.shutdown();
		// END SNIPPET: shutdownServer
	}

	// START SNIPPET: shutdownHook
	private static void registerShutdownHook( final GraphDatabaseService graphDb ) {
		// Registers a shutdown hook for the Neo4j instance so that it
		// shuts down nicely when the VM exits (even if you "Ctrl-C" the
		// running application).
		Runtime.getRuntime().addShutdownHook( new Thread() {
			@Override
			public void run() {
				graphDb.shutdown();
			}
		} );
	}
    
}
