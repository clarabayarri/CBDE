import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.impl.util.FileUtils;


public class DataInserter {

	private Random random = new Random(3l);
	
	private float SF = 0.00333333f;
	
	private static final String DB_PATH = "../neo4j-normalized-db";
	
	private List<Integer> regionIds = new ArrayList<Integer>();
	
    // START SNIPPET: vars
    GraphDatabaseService graphDB;
    Node regionNode;
    Node nationNode;
    Node partNode;
    Node supplierNode;
    Node partSupplierNode;
    Node customerNode;
    Node orderNode;
    Node lineItemNode;
    
    List<Node> regionsNodes = new ArrayList<Node>();
    
    Relationship relationship;
    // END SNIPPET: vars

    // START SNIPPET: createReltype
    private static enum RelTypes implements RelationshipType
    {
        CONTAINS_REGIONS,
        HAS_NATION,
        HAS_CUSTOMER,
        HAS_ORDER,
        HAS_PART,
        HAS_SUPP
    }
    // END SNIPPET: createReltype
    
	public void initialInsert() {
		
		System.out.println("-------- Initial insertion ------");
		
		Date startDate = new Date();
    	createDb();
		Date endDate = new Date();
		
		Long timeDifference = endDate.getTime() - startDate.getTime();
		System.out.println("Insertion took " + timeDifference + " milliseconds.\n");
		
		for ( int i = 0; i < regionsNodes.size(); ++i ) {
			System.out.print( "R_RegionKey  ---  " + regionsNodes.get(i).getProperty( "R_RegionKey" ) + "\n" );
			System.out.print( "R_Name  ---  " + regionsNodes.get(i).getProperty( "R_Name" ) + "\n" );
			System.out.print( "R_Comment  ---  " + regionsNodes.get(i).getProperty( "R_Comment" ) + "\n" );
			System.out.print( "skip  ---  " + regionsNodes.get(i).getProperty( "skip" ) + "\n\n\n" );
		}
	}
	
	private Integer getRandomInteger() {
		// int must have 4 digits
		return random.nextInt(100000 - 1000) + 1000;
	}
	
	private double getRandomDouble( int x ) {
		double sum = random.nextInt(9) + 1;
		for ( int i = 1; i < x/2; ++i ) {
			sum *= 10;
			sum += random.nextInt(10);
		}
		return sum;
	}
	
	private String getRandomString( int size ) {
		String result = "";
		for ( int i = 0; i < size/2; ++i ) {
			int number = random.nextInt(20);
			char chara = (char) ( 'a' + number );
			result += chara;
		}
		return result;
	}
	
	private java.sql.Date getRandomDate() {
		Calendar calendar = new GregorianCalendar();
		calendar.set(2013, 4, 30);
		calendar.add(Calendar.DAY_OF_YEAR, random.nextInt(10000)-5000);
		return new java.sql.Date(calendar.getTimeInMillis());
	}
	
	private void insertRegions(Transaction tx) {
		for ( int i = 1; i <= 5; ++i ) {
			Integer id = getRandomInteger();
			regionNode = graphDB.createNode();
			
			while(regionIds.contains(id)) id = getRandomInteger();
			
			regionNode.setProperty( "R_RegionKey", id );
			// Set one of the region names to the queried value
			if (i == 1)
				regionNode.setProperty( "R_Name", "12345678901234567890123456789012" );
			else
				regionNode.setProperty( "R_Name", getRandomString(64));
			
			regionNode.setProperty( "R_Comment", getRandomString(160));
			regionNode.setProperty( "skip", getRandomString(64));
			
			regionsNodes.add( regionNode );
		}
	}
	
	void createDb() {
        clearDb();
        graphDB = new GraphDatabaseFactory().newEmbeddedDatabase( DB_PATH );
        registerShutdownHook( graphDB );
        Transaction tx = graphDB.beginTx();
        try {
        	insertRegions(tx);
            tx.success();
        }
        finally {
            tx.finish();
        }
    }

    private void clearDb() {
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
