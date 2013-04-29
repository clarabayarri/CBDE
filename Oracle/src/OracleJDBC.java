import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;


public class OracleJDBC {
	
	private static String username;
	private static String password;
	
	private static Connection connection;
	private static DataInserter inserter = new DataInserter();

	public static void main(String[] argv) throws InterruptedException {
		
		if (!checkForDriver()) return;
		if (!loadParameters(argv)) {
			System.out.println("Please provide a username with \"-username\" and a password with \"-password\" to access the DB.");
			return;
		}
		
		if (!connect()) return;
		
		initialInsertion();
		
		// SQL
		
		secondInsertion();
	}

	private static boolean checkForDriver() {
		System.out.println("-------- Oracle JDBC Connection Testing ------");
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
		} catch (ClassNotFoundException e) {
			System.out.println("Where is your Oracle JDBC Driver?\n");
			e.printStackTrace();
			return false;
		}
		System.out.println("Oracle JDBC Driver Found!\n");
		return true;
	}
	
	private static boolean loadParameters(String[] args) {
		for(int i = 0;i < args.length;i++) {
			if ("-username".equals(args[i])) {
				username = args[i+1];
				i++;
			} else if ("-password".equals(args[i])) {
				password = args[i+1];
				i++;
			}
		}
		return (username != null && password != null);
	}
	
	private static boolean connect() {
		try {
			connection = DriverManager.getConnection(
					"jdbc:oracle:thin:@oraclefib.fib.upc.es:1521/ORABD", username, password);
			System.out.println("Connection succeeded\n");
		} catch (SQLException e) {
			System.out.println("Connection Failed! Check output console");
			e.printStackTrace();
			return false;
		}
		return connection != null;
	}
	
	private static void initialInsertion() {
		// From the JDBC application, insert 20.000 lineitem tuples (remember to meet the insertion rules in Appendix B). 
		// Measure the time (i.e., store the time before and after the insertion script).
		try {
			inserter.initialInsert(connection);
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
	}
	
	private static void secondInsertion() {
		// Insert 20.000 lineitem tuples more (do not drop the previous inserted tuples and meet the insertion rules in Appendix B) 
		// and measure the insertion time again.
		try {
			inserter.secondInsert(connection);
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
	}

}
