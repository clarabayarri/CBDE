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
	
	public void initialInsert(Connection connection) throws SQLException {
		connection.setAutoCommit(false);
		System.out.println("-------- Initial insertion ------");
		
		PreparedStatement[] statements = new PreparedStatement[3];
		try {
			statements[0] = insertRegions(connection);
			statements[1] = insertNations(connection);
			statements[2] = insertParts(connection);
			
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
			preparedStatement.setString(2, "12345678901234567890123456789012");
			preparedStatement.setString(3, "12345678901234567890123456789012345678901234567890123456789012345678901234567890");
			preparedStatement.setString(4, "12345678901234567890123456789012");
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
			preparedStatement.setString(2, "12345678901234567890123456789012");
			Integer regionId = regionIds.get(random.nextInt(regionIds.size()));
			preparedStatement.setInt(3,  regionId);
			preparedStatement.setString(4, "12345678901234567890123456789012345678901234567890123456789012345678901234567890");
			preparedStatement.setString(5, "12345678901234567890123456789012");
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
			preparedStatement.setString(2, "12345678901234567890123456789012");
			preparedStatement.setString(3, "12345678901234567890123456789012");
			preparedStatement.setString(4, "12345678901234567890123456789012");
			preparedStatement.setString(5, "12345678901234567890123456789012");
			preparedStatement.setInt(6,  getRandomInteger());
			preparedStatement.setString(7, "12345678901234567890123456789012");
			preparedStatement.setDouble(8, getRandomDouble(13));
			preparedStatement.setString(9, "12345678901234567890123456789012");
			preparedStatement.setString(10, "12345678901234567890123456789012");
			preparedStatement.addBatch();
		}
		
		return preparedStatement;
	}
}
