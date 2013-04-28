import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;


public class OracleJDBC {
	
	private static String username;
	private static String password;
	
	private static Connection connection;

	public static void main(String[] argv) {
		
		if (!checkForDriver()) return;
		if (!loadParameters(argv)) {
			System.out.println("Please provide a username with \"-username\" and a password with \"-password\" to access the DB.");
			return;
		}
		
		if (!connect()) return;
	}

	private static boolean checkForDriver() {
		System.out.println("-------- Oracle JDBC Connection Testing ------");
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
		} catch (ClassNotFoundException e) {
			System.out.println("Where is your Oracle JDBC Driver?");
			e.printStackTrace();
			return false;
		}
		System.out.println("Oracle JDBC Driver Found!");
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
			System.out.println("Connection succeeded");
		} catch (SQLException e) {
			System.out.println("Connection Failed! Check output console");
			e.printStackTrace();
			return false;
		}
		return true;
	}

}
