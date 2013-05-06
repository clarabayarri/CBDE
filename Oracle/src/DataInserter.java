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
	
	public void initialInsert(Connection connection) throws SQLException {
		connection.setAutoCommit(false);
		System.out.println("-------- Initial insertion ------");
		
		PreparedStatement[] statements = new PreparedStatement[8];
		try {
			statements[0] = insertRegions(connection);
			statements[1] = insertNations(connection);
			statements[2] = insertParts(connection);
			statements[3] = insertSuppliers(connection);
			statements[4] = insertPartSuppliers(connection);
			statements[5] = insertCustomers(connection);
			statements[6] = insertOrders(connection);
			statements[7] = insertLineItems(connection);
			
			Date startDate = new Date();
			for (PreparedStatement statement : statements) {
				statement.executeBatch();
			}
			connection.commit();
			
			Date endDate = new Date();
			Long timeDifference = endDate.getTime() - startDate.getTime();
			System.out.println("Insertion took " + timeDifference + " milliseconds.\n");
		} catch (SQLException e) {
			System.out.println(e.getMessage());
			connection.rollback();
		}
	}
	
	public void secondInsert(Connection connection) throws SQLException {
		connection.setAutoCommit(false);
		System.out.println("-------- Second insertion ------");
		
		PreparedStatement[] statements = new PreparedStatement[6];
		try {
			statements[0] = insertParts(connection);
			statements[1] = insertSuppliers(connection);
			statements[2] = insertPartSuppliers(connection);
			statements[3] = insertCustomers(connection);
			statements[4] = insertOrders(connection);
			statements[5] = insertLineItems(connection);
			
			Date startDate = new Date();
			for (PreparedStatement statement : statements) {
				statement.executeBatch();
			}
			connection.commit();
			
			Date endDate = new Date();
			Long timeDifference = endDate.getTime() - startDate.getTime();
			System.out.println("Insertion took " + timeDifference + " milliseconds.\n");
		} catch (SQLException e) {
			System.out.println(e.getMessage());
			connection.rollback();
		}
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
	
	private java.sql.Date getRandomDate() {
		Calendar calendar = new GregorianCalendar();
		calendar.set(2013, 4, 30);
		calendar.add(Calendar.DAY_OF_YEAR, random.nextInt(10000)-5000);
		return new java.sql.Date(calendar.getTimeInMillis());
	}
	
	private PreparedStatement insertRegions(Connection connection) throws SQLException {
		PreparedStatement preparedStatement = null;
		String insertSQL = "INSERT INTO region(R_RegionKey, R_Name, R_Comment, skip) " +
				"VALUES (?,?,?,?)";
		preparedStatement = connection.prepareStatement(insertSQL);
		
		for (int i = 1; i <= 5; ++i) {
			Integer id = getRandomInteger();
			while(regionIds.contains(id)) id = getRandomInteger();
			regionIds.add(id);
			preparedStatement.setInt(1, id);
			// Set one of the region names to the queried value
			if (i == 1) 
				preparedStatement.setString(2, "12345678901234567890123456789012");
			else
				preparedStatement.setString(2, getRandomString(64));
			preparedStatement.setString(3, getRandomString(160));
			preparedStatement.setString(4, getRandomString(64));
			preparedStatement.addBatch();
		}
		
		return preparedStatement;
	}
	
	private PreparedStatement insertNations(Connection connection) throws SQLException {
		PreparedStatement preparedStatement = null;
		String insertSQL = "INSERT INTO nation(N_NationKey, N_Name, N_RegionKey, N_Comment, skip) " +
				"VALUES (?,?,?,?,?)";
		preparedStatement = connection.prepareStatement(insertSQL);
		
		for (int i = 1; i <= 25; ++i) {
			Integer id = getRandomInteger();
			while(nationIds.contains(id)) id = getRandomInteger();
			nationIds.add(id);
			preparedStatement.setInt(1, id);
			preparedStatement.setString(2, getRandomString(64));
			Integer regionId = regionIds.get(random.nextInt(regionIds.size()));
			preparedStatement.setInt(3,  regionId);
			preparedStatement.setString(4, getRandomString(160));
			preparedStatement.setString(5, getRandomString(64));
			preparedStatement.addBatch();
		}
		
		return preparedStatement;
	}
	
	private PreparedStatement insertParts(Connection connection) throws SQLException {
		PreparedStatement preparedStatement = null;
		String insertSQL = "INSERT INTO part(P_PartKey, P_Name, P_Mfgr, P_Brand, P_Type, P_Size, P_Container, P_RetailPrice, P_Comment, skip) " +
				"VALUES (?,?,?,?,?,?,?,?,?,?)";
		preparedStatement = connection.prepareStatement(insertSQL);
		
		int maxValues = (int) (SF * 200000);
		for (int i = 1; i <= maxValues; ++i) {
			Integer id = getRandomInteger();
			while(partIds.contains(id)) id = getRandomInteger();
			partIds.add(id);
			preparedStatement.setInt(1, id);
			preparedStatement.setString(2, getRandomString(64));
			preparedStatement.setString(3, getRandomString(64));
			preparedStatement.setString(4, getRandomString(64));
			// With probability 0.1, set the value to be queried
			if (random.nextInt(10) == 0) {
				preparedStatement.setString(5, "12345678901234567890123456789012");
				preparedStatement.setInt(6, 1000);
			}
			else { 
				preparedStatement.setString(5, getRandomString(64));
				preparedStatement.setInt(6,  getRandomInteger());
			}
				
			preparedStatement.setString(7, getRandomString(64));
			preparedStatement.setDouble(8, getRandomDouble(13));
			preparedStatement.setString(9, getRandomString(64));
			preparedStatement.setString(10, getRandomString(64));
			preparedStatement.addBatch();
		}
		
		return preparedStatement;
	}
	
	private PreparedStatement insertSuppliers(Connection connection) throws SQLException {
		PreparedStatement preparedStatement = null;
		String insertSQL = "INSERT INTO supplier(S_SuppKey, S_Name, S_Address, S_NationKey, S_Phone, S_AcctBal, S_Comment, skip) " +
				"VALUES (?,?,?,?,?,?,?,?)";
		preparedStatement = connection.prepareStatement(insertSQL);
		
		int maxValues = (int) (SF * 10000);
		for (int i = 1; i <= maxValues; ++i) {
			Integer id = getRandomInteger();
			while(supplierIds.contains(id)) id = getRandomInteger();
			supplierIds.add(id);
			preparedStatement.setInt(1, id);
			preparedStatement.setString(2, getRandomString(64));
			preparedStatement.setString(3, getRandomString(64));
			preparedStatement.setInt(4,  nationIds.get(random.nextInt(nationIds.size())));
			preparedStatement.setString(5, getRandomString(18));
			preparedStatement.setDouble(6, getRandomDouble(13));
			preparedStatement.setString(7, getRandomString(105));
			preparedStatement.setString(8, getRandomString(64));
			preparedStatement.addBatch();
		}
		
		return preparedStatement;
	}
	
	private PreparedStatement insertPartSuppliers(Connection connection) throws SQLException {
		PreparedStatement preparedStatement = null;
		String insertSQL = "INSERT INTO partsupp(PS_PartKey, PS_SuppKey, PS_AvailQty, PS_SupplyCost, PS_Comment, skip) " +
				"VALUES (?,?,?,?,?,?)";
		preparedStatement = connection.prepareStatement(insertSQL);
		
		int maxValues = (int) (SF * 800000);
		for (int i = 1; i <= maxValues; ++i) {
			Integer suppid = supplierIds.get(random.nextInt(supplierIds.size()));
			Integer partid = partIds.get(random.nextInt(partIds.size()));
			while(partSuppIds.get(suppid) != null && partSuppIds.get(suppid).contains(partid)) {
				suppid = supplierIds.get(random.nextInt(supplierIds.size()));
				partid = partIds.get(random.nextInt(partIds.size()));
			}
			if (partSuppIds.get(suppid) == null) partSuppIds.put(suppid, new ArrayList<Integer>());
			partSuppIds.get(suppid).add(partid);
			preparedStatement.setInt(1, partid);
			preparedStatement.setInt(2, suppid);
			preparedStatement.setInt(3, getRandomInteger());
			preparedStatement.setDouble(4, getRandomDouble(13));
			preparedStatement.setString(5, getRandomString(200));
			preparedStatement.setString(6, getRandomString(64));
			preparedStatement.addBatch();
		}
		
		return preparedStatement;
	}
	
	private PreparedStatement insertCustomers(Connection connection) throws SQLException {
		PreparedStatement preparedStatement = null;
		String insertSQL = "INSERT INTO customer(C_CustKey, C_Name, C_Address, C_NationKey, C_Phone, C_AcctBal, C_MktSegment, C_Comment, skip) " +
				"VALUES (?,?,?,?,?,?,?,?,?)";
		preparedStatement = connection.prepareStatement(insertSQL);
		
		int maxValues = (int) (SF * 150000);
		for (int i = 1; i <= maxValues; ++i) {
			Integer id = getRandomInteger();
			while(customerIds.contains(id)) id = getRandomInteger();
			customerIds.add(id);
			preparedStatement.setInt(1, id);
			preparedStatement.setString(2, getRandomString(64));
			preparedStatement.setString(3, getRandomString(64));
			preparedStatement.setInt(4, nationIds.get(random.nextInt(nationIds.size())));
			preparedStatement.setString(5, getRandomString(64));
			preparedStatement.setDouble(6, getRandomDouble(13));
			// With probability 0.1, set the value to be queried
			if (random.nextInt(10) == 0)
				preparedStatement.setString(7, "12345678901234567890123456789012");
			else
				preparedStatement.setString(7, getRandomString(64));
			preparedStatement.setString(8, getRandomString(120));
			preparedStatement.setString(9, getRandomString(64));
			preparedStatement.addBatch();
		}
		
		return preparedStatement;
	}
	
	private PreparedStatement insertOrders(Connection connection) throws SQLException {
		PreparedStatement preparedStatement = null;
		String insertSQL = "INSERT INTO orders(O_OrderKey, O_CustKey, O_OrderStatus, O_TotalPrice, O_OrderDate, O_OrderPriority, O_Clerk, O_ShipPriority, O_Comment, skip) " +
				"VALUES (?,?,?,?,?,?,?,?,?,?)";
		preparedStatement = connection.prepareStatement(insertSQL);
		
		int maxValues = (int) (SF * 1500000);
		for (int i = 1; i <= maxValues; ++i) {
			Integer id = getRandomInteger();
			while(orderIds.contains(id)) id = getRandomInteger();
			orderIds.add(id);
			preparedStatement.setInt(1, id);
			preparedStatement.setInt(2, customerIds.get(random.nextInt(customerIds.size())));
			preparedStatement.setString(3, getRandomString(64));
			preparedStatement.setInt(4, getRandomInteger());
			// With probability 0.05, set the date to be queried
			if (random.nextInt(20) == 0) {
				Calendar calendar = new GregorianCalendar(2013,4,29);
				preparedStatement.setDate(5, new java.sql.Date(calendar.getTime().getTime()));
			}
			else
				preparedStatement.setDate(5, getRandomDate());
			preparedStatement.setString(6, getRandomString(15));
			preparedStatement.setString(7, getRandomString(64));
			preparedStatement.setInt(8,getRandomInteger());
			preparedStatement.setString(9, getRandomString(80));
			preparedStatement.setString(10, getRandomString(64));
			preparedStatement.addBatch();
		}
		
		return preparedStatement;
	}
	
	private PreparedStatement insertLineItems(Connection connection) throws SQLException {
		PreparedStatement preparedStatement = null;
		String insertSQL = "INSERT INTO lineitem(L_OrderKey, L_PartKey, L_SuppKey, L_LineNumber, L_Quantity, L_ExtendedPrice, L_Discount, " +
				"L_Tax, L_ReturnFlag, L_LineStatus, L_ShipDate, L_CommitDate, L_ReceiptDate, L_ShipInstruct, L_ShipMode, L_Comment, skip) " +
				"VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		preparedStatement = connection.prepareStatement(insertSQL);
		
		int maxValues = (int) (SF * 6000000);
		for (int i = 1; i <= maxValues; ++i) {
			Integer id = orderIds.get(random.nextInt(orderIds.size()));
			if (lineItemIds.get(id) == null) lineItemIds.put(id, 1000);
			Integer lineId = lineItemIds.get(id) + 1;
			lineItemIds.put(id, lineId);
			preparedStatement.setInt(1, id);
			Integer suppId = supplierIds.get(random.nextInt(supplierIds.size()));
			Integer partId = partSuppIds.get(suppId).get(random.nextInt(partSuppIds.get(suppId).size()));
			preparedStatement.setInt(2, partId);
			preparedStatement.setInt(3, suppId);
			preparedStatement.setInt(4, lineId);
			preparedStatement.setInt(5, getRandomInteger());
			preparedStatement.setDouble(6, getRandomDouble(13));
			preparedStatement.setDouble(7, getRandomDouble(13));
			preparedStatement.setDouble(8, getRandomDouble(13));
			preparedStatement.setString(9, getRandomString(64));
			preparedStatement.setString(10, getRandomString(64));
			// With probability 0.05, set the date to Apr 30 2013
			if (random.nextInt(20) == 0) {
				Calendar calendar = new GregorianCalendar(2013,4,30);
				preparedStatement.setDate(11, new java.sql.Date(calendar.getTime().getTime()));
			}
			else
				preparedStatement.setDate(11, getRandomDate());
			preparedStatement.setDate(12, getRandomDate());
			if (random.nextInt(20) != 0)
				preparedStatement.setDate(13, getRandomDate());
			else
				preparedStatement.setNull(13, java.sql.Types.DATE);
			preparedStatement.setString(14, getRandomString(64));
			preparedStatement.setString(15, getRandomString(64));
			if (random.nextInt(20) != 0)
				preparedStatement.setString(16, getRandomString(64));
			else
				preparedStatement.setNull(16, java.sql.Types.VARCHAR);
			
			preparedStatement.setString(17, getRandomString(64));
			preparedStatement.addBatch();
		}
		
		return preparedStatement;
	}
}
