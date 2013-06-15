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
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexManager;


public class DataInserter {

	private Random random = new Random(3l);

	private float SF = 0.00333333f;

	private List<Integer> regionIds = new ArrayList<Integer>();
	private List<Integer> nationIds = new ArrayList<Integer>();
	private List<Integer> partIds = new ArrayList<Integer>();
	private List<Integer> supplierIds = new ArrayList<Integer>();
	private Map<Integer, List<Integer>> partSuppIds = new HashMap<Integer, List<Integer>>();
	private List<Integer> customerIds = new ArrayList<Integer>();
	private List<Integer> orderIds = new ArrayList<Integer>();
	private Map<Integer, Integer> lineItemIds = new HashMap<Integer, Integer>();

	private List<Node> regions = new ArrayList<Node>();
	private List<Node> nations = new ArrayList<Node>();
	private List<Node> parts = new ArrayList<Node>();
	private List<Node> suppliers = new ArrayList<Node>();
	private Map<Integer, List<Node>> partSupps = new HashMap<Integer, List<Node>>();
	private List<Node> customers = new ArrayList<Node>();
	private List<Node> orders = new ArrayList<Node>();
	private List<Node> lineitems = new ArrayList<Node>();

	Relationship relationship;

	public static enum RelTypes implements RelationshipType
	{
		// Region - Nation
		HAS_NATION,
		// Nation - Supplier
		HAS_SUPPLIER,
		// Part - PartSupp
		PART_HAS_PARTSUPP,
		// Supplier - PartSupp
		SUPPLIER_HAS_PARTSUPP,
		// Nation - Customer
		HAS_CUSTOMER,
		// Customer - Order
		HAS_ORDER,
		// Order - Lineitem
		HAS_LINEITEM,
		// PartSupp - Lineitem
		PARTSUPP_HAS_LINEITEM
	}
	
	private Index<Node> regionsIndex;
	private Index<Node> customersIndex;
	private Index<Node> partIndex;

	public void initialInsert( GraphDatabaseService graphDB ) {

		System.out.println("-------- Initial insertion ------");

		// Indexes
		IndexManager index = graphDB.index();
		regionsIndex = index.forNodes( "regions" );
		customersIndex = index.forNodes("customers");
		partIndex = index.forNodes("parts");
		
		Date startDate = new Date();
		Transaction tx = graphDB.beginTx();
		try {
			insertRegions( tx, graphDB );
			insertNations( tx, graphDB );
			insertParts( tx, graphDB );
			insertSuppliers( tx, graphDB );
			insertPartSuppliers( tx, graphDB );
			insertCustomers( tx, graphDB );
			insertOrders( tx, graphDB );
			insertLineitems( tx, graphDB );
			tx.success();
		}
		finally {
			tx.finish();
		}
		Date endDate = new Date();

		Long timeDifference = endDate.getTime() - startDate.getTime();
		System.out.println("Insertion took " + timeDifference + " milliseconds.\n");
	}

	public void secondInsert( GraphDatabaseService graphDB ) {

		System.out.println("-------- Second insertion ------");

		Date startDate = new Date();
		Transaction tx = graphDB.beginTx();
		try {
			insertParts( tx, graphDB );
			insertSuppliers( tx, graphDB );
			insertPartSuppliers( tx, graphDB );
			insertCustomers( tx, graphDB );
			insertOrders( tx, graphDB );
			insertLineitems( tx, graphDB );
			tx.success();
		}
		finally {
			tx.finish();
		}
		Date endDate = new Date();

		Long timeDifference = endDate.getTime() - startDate.getTime();
		System.out.println("Insertion took " + timeDifference + " milliseconds.\n");
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

	private void insertRegions( Transaction tx, GraphDatabaseService graphDB ) {
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

			// insert into index
			regionsIndex.add( regionNode, "R_Name", regionNode.getProperty( "R_Name" ) );
			
			
			regions.add( regionNode );
		}
	}

	private void insertNations( Transaction tx, GraphDatabaseService graphDB ) {
		// N_NationKey, N_Name, N_RegionKey, N_Comment, skip
		for ( int i = 1; i <= 25; ++i ) {
			Node nationNode = graphDB.createNode();

			Integer id = getRandomInteger();
			while(nationIds.contains(id)) id = getRandomInteger();
			nationIds.add(id);

			nationNode.setProperty("N_NationKey", id);
			nationNode.setProperty("N_Name", getRandomString(64));

			// N_RegionKey
			int index = random.nextInt(regionIds.size());
			Node regionNode = regions.get(index);
			regionNode.createRelationshipTo(nationNode, RelTypes.HAS_NATION);
			//nationNode.createRelationshipTo(regionNode, RelTypes.BELONGS_TO_REGION);

			nationNode.setProperty("N_Comment", getRandomString(160));
			nationNode.setProperty("skip", getRandomString(64));

			nations.add( nationNode );
		}
	}

	private void insertParts( Transaction tx, GraphDatabaseService graphDB ) {
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
			
			// insert into index
			//partIndex.add( partNode, "P_Type", partNode.getProperty( "P_Type" ) );
			partIndex.add( partNode, "P_Size", partNode.getProperty( "P_Size" ) );
			
			parts.add(partNode);
		}
	}

	private void insertSuppliers( Transaction tx, GraphDatabaseService graphDB ) {
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
			//supplierNode.createRelationshipTo(nationNode, RelTypes.BELONGS_TO_NATION);

			supplierNode.setProperty("S_Phone", getRandomString(18));
			supplierNode.setProperty("S_AcctBal", getRandomDouble(13));
			supplierNode.setProperty("S_Comment", getRandomString(105));
			supplierNode.setProperty("skip", getRandomString(64));
			suppliers.add(supplierNode);
		}
	}

	private void insertPartSuppliers( Transaction tx, GraphDatabaseService graphDB ) {
		// PS_PartKey, PS_SuppKey, PS_AvailQty, PS_SupplyCost, PS_Comment, skip

		int maxValues = (int) (SF * 800000);
		for (int i = 1; i <= maxValues; ++i) {
			Node partsuppNode = graphDB.createNode();

			int suppIndex = random.nextInt(supplierIds.size());
			Integer suppid = supplierIds.get(suppIndex);
			int partIndex = random.nextInt(partIds.size());
			Integer partid = partIds.get(partIndex);
			while(partSuppIds.get(suppid) != null && partSuppIds.get(suppid).contains(partid)) {
				suppid = supplierIds.get(random.nextInt(supplierIds.size()));
				partid = partIds.get(random.nextInt(partIds.size()));
			}
			if (partSuppIds.get(suppid) == null) partSuppIds.put(suppid, new ArrayList<Integer>());
			partSuppIds.get(suppid).add(partid);
			//document.put("PS_PartKey", partid);
			Node part = parts.get(partIndex);
			part.createRelationshipTo(partsuppNode, RelTypes.PART_HAS_PARTSUPP);
			//partsuppNode.createRelationshipTo(part, RelTypes.BELONGS_TO_PART);

			// PS_PartKey
			Node supplier = suppliers.get(suppIndex);
			supplier.createRelationshipTo(partsuppNode, RelTypes.SUPPLIER_HAS_PARTSUPP);
			//partsuppNode.createRelationshipTo(supplier, RelTypes.BELONGS_TO_SUPPLIER);

			partsuppNode.setProperty("PS_PartKey", partid);
			partsuppNode.setProperty("PS_AvailQty", getRandomInteger());
			partsuppNode.setProperty("PS_SupplyCost", getRandomDouble(13));
			partsuppNode.setProperty("PS_Comment", getRandomString(200));
			partsuppNode.setProperty("skip", getRandomString(64));

			List<Node> suppParts = partSupps.get(suppid);
			if (suppParts == null) suppParts = new ArrayList<Node>();
			suppParts.add(partsuppNode);
			partSupps.put(suppid, suppParts);
		}
	}

	private void insertCustomers( Transaction tx, GraphDatabaseService graphDB ) {
		// C_CustKey, C_Name, C_Address, C_NationKey, C_Phone, C_AcctBal, C_MktSegment, C_Comment, skip

		int maxValues = (int) (SF * 150000);
		for (int i = 1; i <= maxValues; ++i) {
			Node customerNode = graphDB.createNode();

			Integer id = getRandomInteger();
			while(customerIds.contains(id)) id = getRandomInteger();
			customerIds.add(id);
			customerNode.setProperty("C_CustKey", id);
			customerNode.setProperty("C_Name", getRandomString(64));
			customerNode.setProperty("C_Address", getRandomString(64));

			Node nation = nations.get(random.nextInt(nationIds.size()));
			nation.createRelationshipTo(customerNode, RelTypes.HAS_CUSTOMER);
			//customerNode.createRelationshipTo(nation, RelTypes.CUSTOMER_BELONGS_TO_NATION);

			customerNode.setProperty("C_Phone", getRandomString(64));
			customerNode.setProperty("C_AcctBal", getRandomDouble(13));
			// With probability 0.1, set the value to be queried
			if (random.nextInt(10) == 0)
				customerNode.setProperty("C_MktSegment", "12345678901234567890123456789012");
			else
				customerNode.setProperty("C_MktSegment", getRandomString(64));
			customerNode.setProperty("C_Comment", getRandomString(120));
			customerNode.setProperty("skip", getRandomString(64));
			
			// insert into index
			customersIndex.add( customerNode, "C_MktSegment", customerNode.getProperty( "C_MktSegment" ) );

			customers.add(customerNode);
		}
	}

	private void insertOrders( Transaction tx, GraphDatabaseService graphDB ) {
		// O_OrderKey, O_CustKey, O_OrderStatus, O_TotalPrice, O_OrderDate, O_OrderPriority, O_Clerk, O_ShipPriority, O_Comment, skip

		int maxValues = (int) (SF * 1500000);
		for (int i = 1; i <= maxValues; ++i) {
			Node orderNode = graphDB.createNode();

			Integer id = getRandomInteger();
			while(orderIds.contains(id)) id = getRandomInteger();
			orderIds.add(id);
			orderNode.setProperty("O_OrderKey", id);

			// O_CustKey
			int index = random.nextInt(customerIds.size());
			Node customer = customers.get(index);
			customer.createRelationshipTo(orderNode, RelTypes.HAS_ORDER);
			//orderNode.createRelationshipTo(customer, RelTypes.BELONGS_TO_CUSTOMER);

			orderNode.setProperty("O_OrderStatus", getRandomString(64));
			orderNode.setProperty("O_TotalPrice", getRandomInteger());
			// With probability 0.05, set the date to be queried
			if (random.nextInt(20) == 0) {
				Calendar calendar = new GregorianCalendar(2013,4,29);
				orderNode.setProperty("O_OrderDate", calendar.getTime().getTime());
			}
			else
				orderNode.setProperty("O_OrderDate", getRandomDate().getTime());
			orderNode.setProperty("O_OrderPriority", getRandomString(15));
			orderNode.setProperty("O_Clerk", getRandomString(64));
			orderNode.setProperty("O_ShipPriority", getRandomInteger());
			orderNode.setProperty("O_Comment", getRandomString(80));
			orderNode.setProperty("skip", getRandomString(64));

			orders.add(orderNode);
		}
	}

	private void insertLineitems( Transaction tx, GraphDatabaseService graphDB ) {
		// L_OrderKey, L_PartKey, L_SuppKey, L_LineNumber, L_Quantity, L_ExtendedPrice, L_Discount,
		// L_Tax, L_ReturnFlag, L_LineStatus, L_ShipDate, L_CommitDate, L_ReceiptDate, L_ShipInstruct, L_ShipMode, L_Comment, skip

		int maxValues = (int) (SF * 6000000);
		for (int i = 1; i <= maxValues; ++i) {
			Node lineitemNode = graphDB.createNode();

			// L_OrderKey
			int index = random.nextInt(orderIds.size());
			Node order = orders.get(index);
			order.createRelationshipTo(lineitemNode, RelTypes.HAS_LINEITEM);
			//lineitemNode.createRelationshipTo(order, RelTypes.BELONGS_TO_ORDER);
			
			Integer id = orderIds.get(index);

			if (lineItemIds.get(id) == null) lineItemIds.put(id, 1000);
			Integer lineId = lineItemIds.get(id) + 1;
			lineItemIds.put(id, lineId);
			//document.put("L_OrderKey", id);
			int suppIndex = random.nextInt(supplierIds.size());
			Integer suppId = supplierIds.get(suppIndex);
			int partIndex = random.nextInt(partSuppIds.get(suppId).size());
			Node partSupp = partSupps.get(suppId).get(partIndex);
			partSupp.createRelationshipTo(lineitemNode, RelTypes.PARTSUPP_HAS_LINEITEM);
			lineitemNode.setProperty("L_LineNumber", lineId);
			lineitemNode.setProperty("L_Quantity", getRandomInteger());
			lineitemNode.setProperty("L_ExtendedPrice", getRandomDouble(13));
			lineitemNode.setProperty("L_Discount", getRandomDouble(13));
			lineitemNode.setProperty("L_Tax", getRandomDouble(13));
			lineitemNode.setProperty("L_ReturnFlag", getRandomString(64));
			lineitemNode.setProperty("L_LineStatus", getRandomString(64));
			// With probability 0.05, set the date to Apr 30 2013
			if (random.nextInt(20) == 0) {
				Calendar calendar = new GregorianCalendar(2013,4,30);
				lineitemNode.setProperty("L_ShipDate", calendar.getTime().getTime());
			}
			else
				lineitemNode.setProperty("L_ShipDate", getRandomDate().getTime());
			lineitemNode.setProperty("L_CommitDate", getRandomDate().getTime());
			if (random.nextInt(20) != 0)
				lineitemNode.setProperty("L_ReceiptDate", getRandomDate().getTime());
			lineitemNode.setProperty("L_ShipInstruct", getRandomString(64));
			lineitemNode.setProperty("L_ShipMode", getRandomString(64));
			if (random.nextInt(20) != 0)
				lineitemNode.setProperty("L_Comment", getRandomString(64));

			lineitemNode.setProperty("skip", getRandomString(64));
			lineitems.add(lineitemNode);
		}
	}
}
