import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.List;

public class QuerySet {

	public void executeQueries(Connection connection) throws SQLException {
		System.out.println("-------- Queries ------");

		PreparedStatement statements[] = { 
			query1(connection),
			query2(connection), 
			query3(connection),
			query4(connection)
		};

		for (int i = 0; i < statements.length; ++i) {
			List<Long> timeDifferences = new ArrayList<Long>();
			for (int j = 0; j < 5; ++j) {
				Date startDate = new Date();
				statements[i].executeBatch();
				Date endDate = new Date();

				timeDifferences.add(j, endDate.getTime() - startDate.getTime());
			}

			System.out.println("Query" + (i + 1) + " took "	+ Collections.min(timeDifferences)
					+ " milliseconds as per minimum.\n");
		}
	}

	public PreparedStatement query1(Connection connection) throws SQLException {
		// select for the existing date constraint
		String existingDateQuery = "SELECT l_shipdate FROM lineitem WHERE l_shipdate IS NOT NULL AND ROWNUM = 1";
		Statement statement = connection.createStatement();
		ResultSet rs = statement.executeQuery(existingDateQuery);
		rs.next();
		Date existingDate = rs.getDate("l_shipdate");

		// query1
		String selectSQL = "SELECT l_returnflag, l_linestatus, sum(l_quantity) as sum_qty,"
				+ " sum(l_extendedprice) as sum_base_price, sum(l_extendedprice*(1-l_discount)) as "
				+ " sum_disc_price, sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge, "
				+ " avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) "
				+ " as avg_disc, count(*) as count_order "
				+ " FROM lineitem "
				+ " WHERE l_shipdate <= ? "
				+ " GROUP BY l_returnflag, l_linestatus "
				+ " ORDER BY l_returnflag, l_linestatus";

		PreparedStatement preparedStatement = connection.prepareStatement(selectSQL);
		preparedStatement.setDate(1, new java.sql.Date(existingDate.getTime()));

		return preparedStatement;
	}

	public PreparedStatement query2(Connection connection) throws SQLException {
		// select for the existing size and type constraints
		String existingSizeAndTypesQuery = "SELECT p_size, p_type FROM part WHERE ROWNUM = 1";
		Statement statement = connection.createStatement();
		ResultSet rs = statement.executeQuery(existingSizeAndTypesQuery);
		rs.next();
		Integer existingSize = rs.getInt("p_size");
		String existingType = rs.getString("p_type");

		// select for the existing region constraint
		String existingregionQuery = "SELECT r_name FROM region WHERE ROWNUM = 1";
		statement = connection.createStatement();
		rs = statement.executeQuery(existingregionQuery);
		rs.next();
		String existingRegion = rs.getString("r_name");

		// query2
		String selectSQL = "SELECT s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment "
				+ " FROM part, supplier, partsupp, nation, region "
				+ " WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey AND p_size = ? "
				+ " AND p_type like ? AND s_nationkey = n_nationkey AND n_regionkey = "
				+ " r_regionkey AND r_name = ? AND ps_supplycost = (SELECT "
				+ " min(ps_supplycost) FROM partsupp, supplier, nation, region WHERE p_partkey = "
				+ " ps_partkey AND s_suppkey = ps_suppkey AND s_nationkey = n_nationkey AND "
				+ " n_regionkey = r_regionkey AND r_name = ?) "
				+ " ORDER BY s_acctbal desc, n_name, s_name, p_partkey";

		PreparedStatement preparedStatement = connection.prepareStatement(selectSQL);
		preparedStatement.setInt(1, existingSize);
		preparedStatement.setString(2, existingType);
		preparedStatement.setString(3, existingRegion);
		preparedStatement.setString(4, existingRegion);

		return preparedStatement;
	}

	public PreparedStatement query3(Connection connection) throws SQLException {
		// select for the existing segment constraint
		String existingSegmentQuery = "SELECT c_mktsegment FROM customer WHERE ROWNUM = 1";
		Statement statement = connection.createStatement();
		ResultSet rs = statement.executeQuery(existingSegmentQuery);
		rs.next();
		String existingSegment = rs.getString("c_mktsegment");

		// select for the existing lineitem date constraint
		String existingLineitemDateQuery = "SELECT l_shipdate FROM lineitem WHERE l_shipdate IS NOT NULL AND ROWNUM = 1";
		statement = connection.createStatement();
		rs = statement.executeQuery(existingLineitemDateQuery);
		rs.next();
		Date existingLineitemDate = rs.getDate("l_shipdate");
		
		// select for the existing orders date constraint
		String existingOrdersDateQuery = "SELECT o_orderdate FROM orders WHERE ROWNUM = 1";
		statement = connection.createStatement();
		rs = statement.executeQuery(existingOrdersDateQuery);
		rs.next();
		Date existingOrdersDate = rs.getDate("o_orderdate");
		
		// query3
		String selectSQL = "SELECT l_orderkey, sum(l_extendedprice*(1-l_discount)) as revenue, "
				+ " o_orderdate, o_shippriority "
				+ " FROM customer, orders, lineitem "
				+ " WHERE c_mktsegment = ? AND c_custkey = o_custkey AND l_orderkey = "
				+ " o_orderkey AND o_orderdate < ? AND l_shipdate > ? "
				+ " GROUP BY l_orderkey, o_orderdate, o_shippriority "
				+ " ORDER BY revenue desc, o_orderdate";

		PreparedStatement preparedStatement = connection.prepareStatement(selectSQL);
		preparedStatement.setString(1, existingSegment);
		preparedStatement.setDate(1, new java.sql.Date(existingOrdersDate.getTime()));
		preparedStatement.setDate(1, new java.sql.Date(existingLineitemDate.getTime()));
		
		return preparedStatement;
	}

	 public PreparedStatement query4(Connection connection) throws SQLException {
		// select for the existing region constraint
		String existingregionQuery = "SELECT r_name FROM region WHERE ROWNUM = 1";
		Statement statement = connection.createStatement();
		ResultSet rs = statement.executeQuery(existingregionQuery);
		rs.next();
		String existingRegion = rs.getString("r_name");
		
		// select for the existing orders date constraint
		String existingOrdersDateQuery = "SELECT o_orderdate FROM orders WHERE ROWNUM < 3";
		statement = connection.createStatement();
		rs = statement.executeQuery(existingOrdersDateQuery);
		List <Date> existingOrdersDate = new ArrayList<Date>();
		while (rs.next()) {
			existingOrdersDate.add(rs.getDate("o_orderdate"));
		}
		Date existingMinOrdersDate = Collections.min(existingOrdersDate);
		Calendar date = Calendar.getInstance();  
	    date.setTime(Collections.max(existingOrdersDate));
	    date.add(Calendar.YEAR,1);
		Date existingMaxOrdersDate = date.getTime();
		
		// query4
		String selectSQL = "SELECT n_name, sum(l_extendedprice * (1 - l_discount)) as revenue "
				+ " FROM customer, orders, lineitem, supplier, nation, region "
				+ " WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey AND l_suppkey = "
				+ " s_suppkey AND c_nationkey = s_nationkey AND s_nationkey = n_nationkey AND "
				+ " n_regionkey = r_regionkey AND r_name = ? AND o_orderdate >= ? "
				+ " AND o_orderdate < ? "
				+ " GROUP BY n_name "
				+ " ORDER BY revenue desc";

		PreparedStatement preparedStatement = connection.prepareStatement(selectSQL);
		preparedStatement.setString(1, existingRegion);
		preparedStatement.setDate(1, new java.sql.Date(existingMinOrdersDate.getTime()));
		preparedStatement.setDate(1, new java.sql.Date(existingMaxOrdersDate.getTime()));
		
		return preparedStatement;
	 }
	 
}
