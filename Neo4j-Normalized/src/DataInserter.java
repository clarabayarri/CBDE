import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

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
	private List<Integer> nationIds = new ArrayList<Integer>();
	private List<Integer> partIds = new ArrayList<Integer>();
	private List<Integer> supplierIds = new ArrayList<Integer>();
	private Map<Integer, List<Integer>> partSuppIds = new HashMap<Integer, List<Integer>>();
	private List<Integer> customerIds = new ArrayList<Integer>();
	private List<Integer> orderIds = new ArrayList<Integer>();
	private Map<Integer, Integer> lineItemIds = new HashMap<Integer, Integer>();

	GraphDatabaseService graphDB;

	private List<Node> regions = new ArrayList<Node>();
	private List<Node> nations = new ArrayList<Node>();
	private List<Node> parts = new ArrayList<Node>();
	private List<Node> suppliers = new ArrayList<Node>();
	private Map<Integer, List<Node>> partSupps = new HashMap<Integer, List<Node>>();
	private List<Node> customers = new ArrayList<Node>();
	private List<Node> orders = new ArrayList<Node>();
	private List<Node> lineitems = new ArrayList<Node>();

	Relationship relationship;
	
	private static enum RelTypes implements RelationshipType
	{
		// Region - Nation
		BELONGS_TO_REGION,
		HAS_NATION,
		// Nation - Supplier
		BELONGS_TO_NATION,
		HAS_SUPPLIER
	}

	public void initialInsert() {

		System.out.println("-------- Initial insertion ------");

		Date startDate = new Date();
		createDb();
		Date endDate = new Date();

		Long timeDifference = endDate.getTime() - startDate.getTime();
		System.out.println("Insertion took " + timeDifference + " milliseconds.\n");

		for ( int i = 0; i < regions.size(); ++i ) {
			System.out.print( "R_RegionKey  ---  " + regions.get(i).getProperty( "R_RegionKey" ) + "\n" );
			System.out.print( "R_Name  ---  " + regions.get(i).getProperty( "R_Name" ) + "\n" );
			System.out.print( "R_Comment  ---  " + regions.get(i).getProperty( "R_Comment" ) + "\n" );
			System.out.print( "skip  ---  " + regions.get(i).getProperty( "skip" ) + "\n" );
			for (Relationship nation : regions.get(i).getRelationships()) {
				System.out.println("\tRelated to " + nation.getEndNode().getProperty("N_NationKey"));
			}
			System.out.println();
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
			Node regionNode = graphDB.createNode();

			while(regionIds.contains(id)) id = getRandomInteger();
			regionIds.add(id);

			regionNode.setProperty( "R_RegionKey", id );
			// Set one of the region names to the queried value
			if (i == 1)
				regionNode.setProperty( "R_Name", "12345678901234567890123456789012" );
			else
				regionNode.setProperty( "R_Name", getRandomString(64));

			regionNode.setProperty( "R_Comment", getRandomString(160));
			regionNode.setProperty( "skip", getRandomString(64));

			regions.add( regionNode );
		}
	}

	private void insertNations(Transaction tx) {
		// N_NationKey, N_Name, N_RegionKey, N_Comment, skip
		for ( int i = 1; i <= 25; ++i ) {
			Node nationNode = graphDB.createNode();

			Integer id = getRandomInteger();
			while(nationIds.contains(id)) id = getRandomInteger();
			nationIds.add(id);
			nations.add( nationNode );

			nationNode.setProperty("N_NationKey", id);
			nationNode.setProperty("N_Name", getRandomString(64));

			// N_RegionKey
			int index = random.nextInt(regionIds.size());
			Node regionNode = regions.get(index);
			regionNode.createRelationshipTo(nationNode, RelTypes.HAS_NATION);
			nationNode.createRelationshipTo(regionNode, RelTypes.BELONGS_TO_REGION);

			nationNode.setProperty("N_Comment", getRandomString(160));
			nationNode.setProperty("skip", getRandomString(64));
		}
	}

	private void insertParts(Transaction tx) {
		// P_PartKey, P_Name, P_Mfgr, P_Brand, P_Type, P_Size, P_Container, P_RetailPrice, P_Comment, skip

		int maxValues = (int) (SF * 200000);
		for (int i = 1; i <= maxValues; ++i) {
			Node partNode = graphDB.createNode();

			Integer id = getRandomInteger();
			while(partIds.contains(id)) id = getRandomInteger();
			partIds.add(id);
			partNode.setProperty("P_PartKey", id);
			partNode.setProperty("P_Name", getRandomString(64));
			partNode.setProperty("P_Mfgr", getRandomString(64));
			partNode.setProperty("P_Brand", getRandomString(64));
			// With probability 0.1, set the value to be queried
			if (random.nextInt(10) == 0) {
				partNode.setProperty("P_Type", "12345678901234567890123456789012");
				partNode.setProperty("P_Size", 1000);
			}
			else { 
				partNode.setProperty("P_Type", getRandomString(64));
				partNode.setProperty("P_Size",  getRandomInteger());
			}

			partNode.setProperty("P_Container", getRandomString(64));
			partNode.setProperty("P_RetailPrice", getRandomDouble(13));
			partNode.setProperty("P_Comment", getRandomString(64));
			partNode.setProperty("skip", getRandomString(64));
			parts.add(partNode);
		}
	}

	private void insertSuppliers(Transaction tx) {
		// S_SuppKey, S_Name, S_Address, S_NationKey, S_Phone, S_AcctBal, S_Comment, skip

		int maxValues = (int) (SF * 10000);
		for (int i = 1; i <= maxValues; ++i) {
			Node supplierNode = graphDB.createNode();

			Integer id = getRandomInteger();
			while(supplierIds.contains(id)) id = getRandomInteger();
			supplierIds.add(id);
			supplierNode.setProperty("S_SuppKey", id);
			supplierNode.setProperty("S_Name", getRandomString(64));
			supplierNode.setProperty("S_Address", getRandomString(64));

			// S_NationKey
			int index = random.nextInt(nationIds.size());
			Node nationNode = nations.get(index);
			nationNode.createRelationshipTo(supplierNode, RelTypes.HAS_SUPPLIER);
			supplierNode.createRelationshipTo(nationNode, RelTypes.BELONGS_TO_NATION);

			supplierNode.setProperty("S_Phone", getRandomString(18));
			supplierNode.setProperty("S_AcctBal", getRandomDouble(13));
			supplierNode.setProperty("S_Comment", getRandomString(105));
			supplierNode.setProperty("skip", getRandomString(64));
			suppliers.add(supplierNode);
		}
	}

	void createDb() {
		clearDb();
		graphDB = new GraphDatabaseFactory().newEmbeddedDatabase( DB_PATH );
		registerShutdownHook( graphDB );
		Transaction tx = graphDB.beginTx();
		try {
			insertRegions(tx);
			insertNations(tx);
			insertParts(tx);
			insertSuppliers(tx);
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
