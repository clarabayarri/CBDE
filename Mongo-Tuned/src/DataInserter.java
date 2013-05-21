import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.WriteConcern;


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
	private List<BasicDBObject> regions = new ArrayList<BasicDBObject>();
	private List<BasicDBObject> nations = new ArrayList<BasicDBObject>();
	private List<BasicDBObject> parts = new ArrayList<BasicDBObject>();
	private List<BasicDBObject> suppliers = new ArrayList<BasicDBObject>();
	private Map<Integer, List<BasicDBObject>> partSupps = new HashMap<Integer, List<BasicDBObject>>();
	private List<BasicDBObject> customers = new ArrayList<BasicDBObject>();
	private List<BasicDBObject> orders = new ArrayList<BasicDBObject>();
	private List<BasicDBObject> lineitems = new ArrayList<BasicDBObject>();
	
	public void initialInsert(DB database) {
		System.out.println("-------- Initial insertion ------");
		
		getRegionObjects();
		getNationObjects();
		getPartObjects();
		getSupplierObjects();
		getPartSupplierObjects();
		getCustomerObjects();
		getOrderObjects();
		getLineitemObjects();
		
		Map<DBCollection, List<BasicDBObject>> inserts = new HashMap<DBCollection, List<BasicDBObject>>();
		inserts.put(database.getCollection("region"), regions);
		inserts.put(database.getCollection("part"), parts);
		inserts.put(database.getCollection("customer"), customers);
		inserts.put(database.getCollection("lineitem"), lineitems);
		
		Date startDate = new Date();
		for (DBCollection collection : inserts.keySet()) {
			List<BasicDBObject> individualInserts = inserts.get(collection);
			for (BasicDBObject object : individualInserts) {
				collection.save(object, WriteConcern.NORMAL);
			}
		}
		Date endDate = new Date();
		
		Long timeDifference = endDate.getTime() - startDate.getTime();
		System.out.println("Insertion took " + timeDifference + " milliseconds.\n");
	}
	
	public void secondInsert(DB database) {
		System.out.println("-------- Second insertion ------");
		
		getPartObjects();
		getSupplierObjects();
		getPartSupplierObjects();
		getCustomerObjects();
		getOrderObjects();
		getLineitemObjects();
		
		Map<DBCollection, List<BasicDBObject>> inserts = new HashMap<DBCollection, List<BasicDBObject>>();
		inserts.put(database.getCollection("region"), regions);
		inserts.put(database.getCollection("part"), parts);
		inserts.put(database.getCollection("customer"), customers);
		inserts.put(database.getCollection("lineitem"), lineitems);
		
		Date startDate = new Date();
		for (DBCollection collection : inserts.keySet()) {
			List<BasicDBObject> individualInserts = inserts.get(collection);
			for (BasicDBObject object : individualInserts) {
				collection.save(object, WriteConcern.NORMAL);
			}
		}
		Date endDate = new Date();
		
		Long timeDifference = endDate.getTime() - startDate.getTime();
		System.out.println("Insertion took " + timeDifference + " milliseconds.\n");
	}
	
	private Integer getRandomInteger() {
		// int must have 4 digits
		return random.nextInt(100000 - 1000) + 1000;
	}
	
	private double getRandomDouble(int x) {
		double sum = random.nextInt(9) + 1;
		for (int i = 1; i < x/2; ++i) {
			sum *= 10;
			sum += random.nextInt(10);
		}
		return sum;
	}
	
	private String getRandomString(int size) {
		String result = "";
		for (int i = 0; i < size/2; ++i) {
			int number = random.nextInt(20);
			char chara = (char) ('a' + number);
			result += chara;
		}
		return result;
	}
	
	private Date getRandomDate() {
		Calendar calendar = new GregorianCalendar();
		calendar.set(2013, 4, 30);
		calendar.add(Calendar.DAY_OF_YEAR, random.nextInt(10000)-5000);
		return calendar.getTime();
	}
	
	private void getRegionObjects() {
		// R_RegionKey, R_Name, R_Comment, skip
		for (int i = 1; i <= 5; ++i) {
			BasicDBObject document = new BasicDBObject();
			
			Integer id = getRandomInteger();
			while(regionIds.contains(id)) id = getRandomInteger();
			regionIds.add(id);
			regions.add(document);
			document.put("_id", id);
			if (i == 1) 
				document.put("R_Name", "12345678901234567890123456789012");
			else
				document.put("R_Name", getRandomString(64));
			document.put("R_Comment", getRandomString(160));
			document.put("skip", getRandomString(64));
			document.put("nations", new ArrayList<BasicDBObject>());
		}
	}
	
	@SuppressWarnings("unchecked")
	private void getNationObjects() {
		// N_NationKey, N_Name, N_RegionKey, N_Comment, skip
		for (int i = 1; i <= 25; ++i) {
			BasicDBObject document = new BasicDBObject();
			
			Integer id = getRandomInteger();
			while(nationIds.contains(id)) id = getRandomInteger();
			nationIds.add(id);
			nations.add(document);
			document.put("N_NationKey", id);
			document.put("N_Name", getRandomString(64));
			
			// N_RegionKey
			int index = random.nextInt(regionIds.size());
			List<BasicDBObject> regionNations = (List<BasicDBObject>) regions.get(index).get("nations");
			regionNations.add(document);
			regions.get(index).put("nations", regionNations);
			
			document.put("N_Comment", getRandomString(160));
			document.put("skip", getRandomString(64));
			document.put("suppliers", new ArrayList<BasicDBObject>());
		}
	}
	
	private void getPartObjects() {
		// P_PartKey, P_Name, P_Mfgr, P_Brand, P_Type, P_Size, P_Container, P_RetailPrice, P_Comment, skip
		
		int maxValues = (int) (SF * 200000);
		for (int i = 1; i <= maxValues; ++i) {
			BasicDBObject document = new BasicDBObject();
			
			Integer id = getRandomInteger();
			while(partIds.contains(id)) id = getRandomInteger();
			partIds.add(id);
			document.put("_id", id);
			document.put("P_Name", getRandomString(64));
			document.put("P_Mfgr", getRandomString(64));
			document.put("P_Brand", getRandomString(64));
			// With probability 0.1, set the value to be queried
			if (random.nextInt(10) == 0) {
				document.put("P_Type", "12345678901234567890123456789012");
				document.put("P_Size", 1000);
			}
			else { 
				document.put("P_Type", getRandomString(64));
				document.put("P_Size",  getRandomInteger());
			}
				
			document.put("P_Container", getRandomString(64));
			document.put("P_RetailPrice", getRandomDouble(13));
			document.put("P_Comment", getRandomString(64));
			document.put("skip", getRandomString(64));
			parts.add(document);
		}
	}
	
	@SuppressWarnings("unchecked")
	private void getSupplierObjects() {
		// S_SuppKey, S_Name, S_Address, S_NationKey, S_Phone, S_AcctBal, S_Comment, skip
		
		int maxValues = (int) (SF * 10000);
		for (int i = 1; i <= maxValues; ++i) {
			BasicDBObject document = new BasicDBObject();
			
			Integer id = getRandomInteger();
			while(supplierIds.contains(id)) id = getRandomInteger();
			supplierIds.add(id);
			document.put("S_SuppKey", id);
			document.put("S_Name", getRandomString(64));
			document.put("S_Address", getRandomString(64));
			
			// S_NationKey
			int index = random.nextInt(nationIds.size());
			List<BasicDBObject> nationSuppliers = (List<BasicDBObject>) nations.get(index).get("suppliers");
			nationSuppliers.add(document);
			nations.get(index).put("suppliers", nationSuppliers);
			
			document.put("S_Phone", getRandomString(18));
			document.put("S_AcctBal", getRandomDouble(13));
			document.put("S_Comment", getRandomString(105));
			document.put("skip", getRandomString(64));
			document.put("partsupps", new ArrayList<BasicDBObject>());
			suppliers.add(document);
		}
	}
	
	@SuppressWarnings("unchecked")
	private void getPartSupplierObjects() {
		// PS_PartKey, PS_SuppKey, PS_AvailQty, PS_SupplyCost, PS_Comment, skip
		
		int maxValues = (int) (SF * 800000);
		for (int i = 1; i <= maxValues; ++i) {
			BasicDBObject document = new BasicDBObject();
			
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
			
			// PS_PartKey
			List<BasicDBObject> partSuppliers = (List<BasicDBObject>) suppliers.get(suppIndex).get("partsupps");
			partSuppliers.add(document);
			suppliers.get(suppIndex).put("partsupps", partSuppliers);
			
			document.put("PS_PartKey", partid);
			document.put("PS_AvailQty", getRandomInteger());
			document.put("PS_SupplyCost", getRandomDouble(13));
			document.put("PS_Comment", getRandomString(200));
			document.put("skip", getRandomString(64));
			List<BasicDBObject> suppParts = partSupps.get(suppid);
			if (suppParts == null) suppParts = new ArrayList<BasicDBObject>();
			suppParts.add(document);
			partSupps.put(suppid, suppParts);
		}
	}
	
	private void getCustomerObjects() {
		// C_CustKey, C_Name, C_Address, C_NationKey, C_Phone, C_AcctBal, C_MktSegment, C_Comment, skip
		
		int maxValues = (int) (SF * 150000);
		for (int i = 1; i <= maxValues; ++i) {
			BasicDBObject document = new BasicDBObject();
			
			Integer id = getRandomInteger();
			while(customerIds.contains(id)) id = getRandomInteger();
			customerIds.add(id);
			document.put("_id", id);
			document.put("C_Name", getRandomString(64));
			document.put("C_Address", getRandomString(64));
			document.put("C_NationKey", nationIds.get(random.nextInt(nationIds.size())));
			document.put("C_Phone", getRandomString(64));
			document.put("C_AcctBal", getRandomDouble(13));
			// With probability 0.1, set the value to be queried
			if (random.nextInt(10) == 0)
				document.put("C_MktSegment", "12345678901234567890123456789012");
			else
				document.put("C_MktSegment", getRandomString(64));
			document.put("C_Comment", getRandomString(120));
			document.put("skip", getRandomString(64));
			document.put("orders", new ArrayList<BasicDBObject>());
			customers.add(document);
		}
	}
	
	@SuppressWarnings("unchecked")
	private void getOrderObjects() {
		// O_OrderKey, O_CustKey, O_OrderStatus, O_TotalPrice, O_OrderDate, O_OrderPriority, O_Clerk, O_ShipPriority, O_Comment, skip
		
		int maxValues = (int) (SF * 1500000);
		for (int i = 1; i <= maxValues; ++i) {
			BasicDBObject document = new BasicDBObject();
			
			Integer id = getRandomInteger();
			while(orderIds.contains(id)) id = getRandomInteger();
			orderIds.add(id);
			document.put("O_OrderKey", id);
			
			// O_CustKey
			int index = random.nextInt(customerIds.size());
			List<BasicDBObject> customerOrders = (List<BasicDBObject>) customers.get(index).get("orders");
			customerOrders.add(document);
			customers.get(index).put("orders", customerOrders);
			
			document.put("O_OrderStatus", getRandomString(64));
			document.put("O_TotalPrice", getRandomInteger());
			// With probability 0.05, set the date to be queried
			if (random.nextInt(20) == 0) {
				Calendar calendar = new GregorianCalendar(2013,4,29);
				document.put("O_OrderDate", new java.sql.Date(calendar.getTime().getTime()));
			}
			else
				document.put("O_OrderDate", getRandomDate());
			document.put("O_OrderPriority", getRandomString(15));
			document.put("O_Clerk", getRandomString(64));
			document.put("O_ShipPriority", getRandomInteger());
			document.put("O_Comment", getRandomString(80));
			document.put("skip", getRandomString(64));
			document.put("lineitems", new ArrayList<BasicDBObject>());
			orders.add(document);
		}
	}
	
	@SuppressWarnings("unchecked")
	private void getLineitemObjects() {
		// We only use these objects to insert, so no need to keep the old ones.
		lineitems.clear();
		
		// L_OrderKey, L_PartKey, L_SuppKey, L_LineNumber, L_Quantity, L_ExtendedPrice, L_Discount,
		// L_Tax, L_ReturnFlag, L_LineStatus, L_ShipDate, L_CommitDate, L_ReceiptDate, L_ShipInstruct, L_ShipMode, L_Comment, skip
		
		int maxValues = (int) (SF * 6000000);
		for (int i = 1; i <= maxValues; ++i) {
			BasicDBObject document = new BasicDBObject();
			
			// L_OrderKey
			int index = random.nextInt(orderIds.size());
			Integer id = orderIds.get(index);
			List<BasicDBObject> orderLines = (List<BasicDBObject>) orders.get(index).get("lineitems");
			orderLines.add(document);
			orders.get(index).put("lineitems", orderLines);
			
			if (lineItemIds.get(id) == null) lineItemIds.put(id, 1000);
			Integer lineId = lineItemIds.get(id) + 1;
			lineItemIds.put(id, lineId);
			//document.put("L_OrderKey", id);
			Integer suppId = supplierIds.get(random.nextInt(supplierIds.size()));
			Integer partId = partSuppIds.get(suppId).get(random.nextInt(partSuppIds.get(suppId).size()));
			document.put("L_PartKey", partId);
			document.put("L_SuppKey", suppId);
			document.put("L_LineNumber", lineId);
			document.put("L_Quantity", getRandomInteger());
			document.put("L_ExtendedPrice", getRandomDouble(13));
			document.put("L_Discount", getRandomDouble(13));
			document.put("L_Tax", getRandomDouble(13));
			document.put("L_ReturnFlag", getRandomString(64));
			document.put("L_LineStatus", getRandomString(64));
			// With probability 0.05, set the date to Apr 30 2013
			if (random.nextInt(20) == 0) {
				Calendar calendar = new GregorianCalendar(2013,4,30);
				document.put("L_ShipDate", new java.sql.Date(calendar.getTime().getTime()));
			}
			else
				document.put("L_ShipDate", getRandomDate());
			document.put("L_CommitDate", getRandomDate());
			if (random.nextInt(20) != 0)
				document.put("L_ReceiptDate", getRandomDate());
			document.put("L_ShipInstruct", getRandomString(64));
			document.put("L_ShipMode", getRandomString(64));
			if (random.nextInt(20) != 0)
				document.put("L_Comment", getRandomString(64));
			
			document.put("skip", getRandomString(64));
			lineitems.add(document);
		}
	}
}
