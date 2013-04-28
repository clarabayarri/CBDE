
public class OracleJDBC {

	public static void main(String[] argv) {
		
		if (!checkForDriver()) return;

		
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

}
