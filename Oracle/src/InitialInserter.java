import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;


public class InitialInserter {

	private Random random = new Random();
	
	private float SF = 0.00333333f;
	
	private List<Integer> regionIds = new ArrayList<Integer>();
	private List<Integer> nationIds = new ArrayList<Integer>();
	private List<Integer> partIds = new ArrayList<Integer>();
	private List<Integer> supplierIds = new ArrayList<Integer>();
	
	public void initialInsert(Connection connection) throws SQLException {
		connection.setAutoCommit(false);
		System.out.println("-------- Initial insertion ------");
		
		PreparedStatement[] statements = new PreparedStatement[4];
		try {
			statements[0] = insertRegions(connection);
			statements[1] = insertNations(connection);
			statements[2] = insertParts(connection);
			statements[3] = insertSuppliers(connection);
			
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
		return random.nextInt(10000 - 1000) + 1000;
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
			preparedStatement.setString(5, getRandomString(64));
			preparedStatement.setInt(6,  getRandomInteger());
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
}
