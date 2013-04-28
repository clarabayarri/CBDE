import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Random;


public class InitialInserter {

	private Random random = new Random();
	
	public void initialInsert(Connection connection) throws SQLException {
		try {
			insertRegions(connection);
			insertNations(connection);
			connection.commit();
		} catch (SQLException e) {
			System.out.println(e.getMessage());
			connection.rollback();
		}
	}
	
	private void insertRegions(Connection connection) throws SQLException {
		PreparedStatement preparedStatement = null;
		String insertSQL = "INSERT INTO region(R_RegionKey, R_Name, R_Comment, skip) " +
				"VALUES (?,?,?,?)";
		preparedStatement = connection.prepareStatement(insertSQL);
		connection.setAutoCommit(false);
		
		for (int i = 1; i <= 5; ++i) {
			preparedStatement.setInt(1, 1534 + i);
			preparedStatement.setString(2, "12345678901234567890123456789012");
			preparedStatement.setString(3, "12345678901234567890123456789012345678901234567890123456789012345678901234567890");
			preparedStatement.setString(4, "12345678901234567890123456789012");
			preparedStatement.addBatch();
		}
		
		preparedStatement.executeBatch();
	}
	
	private void insertNations(Connection connection) throws SQLException {
		PreparedStatement preparedStatement = null;
		String insertSQL = "INSERT INTO nation(N_NationKey, N_Name, N_RegionKey, N_Comment, skip) " +
				"VALUES (?,?,?,?,?)";
		preparedStatement = connection.prepareStatement(insertSQL);
		connection.setAutoCommit(false);
		
		for (int i = 1; i <= 25; ++i) {
			preparedStatement.setInt(1, 1534 + i);
			preparedStatement.setString(2, "12345678901234567890123456789012");
			preparedStatement.setInt(3,  1534 + random.nextInt(5) + 1);
			preparedStatement.setString(4, "12345678901234567890123456789012345678901234567890123456789012345678901234567890");
			preparedStatement.setString(5, "12345678901234567890123456789012");
			preparedStatement.addBatch();
		}
		
		preparedStatement.executeBatch();
	}
}
