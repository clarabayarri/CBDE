import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class QuerySet {

	public void executeQueries(Connection connection) throws SQLException {
		connection.setAutoCommit(false);
		System.out.println("-------- Queries ------");

		PreparedStatement statements[] = { 
			query1(connection),
			query2(connection), 
			query3(connection),
			query4(connection)
		};

		Long average = (long) 0;
		for (int i = 0; i < statements.length; ++i) {
			List<Long> timeDifferences = new ArrayList<Long>();
			for (int j = 0; j < 5; ++j) {
				Long start = System.nanoTime();
				statements[i].executeBatch();
				connection.commit();
				Long end = System.nanoTime();

				timeDifferences.add(j, end-start);
			}
			
			System.out.println("Query" + (i + 1) + " took " + timeDifferences + 
					" in nanoseconds --- with minimum " + Collections.min(timeDifferences));
			average += Collections.min(timeDifferences);
		}
		System.out.println("\nAverage query time " + average/statements.length + " in nanoseconds\n");
	}

	public PreparedStatement query1(Connection connection) throws SQLException {
		// query1
		String selectSQL = "SELECT l_returnflag, l_linestatus, sum(l_quantity) as sum_qty,"
				+ " sum(l_extendedprice) as sum_base_price, sum(l_extendedprice*(1-l_discount)) as "
				+ " sum_disc_price, sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge, "
				+ " avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) "
				+ " as avg_disc, count(*) as count_order "
				+ " FROM lineitem "
				+ " WHERE l_shipdate <= '30-APR-13' "
				+ " GROUP BY l_returnflag, l_linestatus "
				+ " ORDER BY l_returnflag, l_linestatus";

		PreparedStatement preparedStatement = connection.prepareStatement(selectSQL);

		return preparedStatement;
	}

	public PreparedStatement query2(Connection connection) throws SQLException {
		// query2
		String selectSQL = "SELECT s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment "
				+ " FROM part, supplier, partsupp, nation, region "
				+ " WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey AND p_size = 1000 "
				+ " AND p_type like '%0123456789012' AND s_nationkey = n_nationkey AND n_regionkey = "
				+ " r_regionkey AND r_name = '12345678901234567890123456789012' AND ps_supplycost = (SELECT "
				+ " min(ps_supplycost) FROM partsupp, supplier, nation, region WHERE p_partkey = "
				+ " ps_partkey AND s_suppkey = ps_suppkey AND s_nationkey = n_nationkey AND "
				+ " n_regionkey = r_regionkey AND r_name = '12345678901234567890123456789012') "
				+ " ORDER BY s_acctbal desc, n_name, s_name, p_partkey";

		PreparedStatement preparedStatement = connection.prepareStatement(selectSQL);

		return preparedStatement;
	}

	public PreparedStatement query3(Connection connection) throws SQLException {
		// query3
		String selectSQL = "SELECT l_orderkey, sum(l_extendedprice*(1-l_discount)) as revenue, "
				+ " o_orderdate, o_shippriority "
				+ " FROM customer, orders, lineitem "
				+ " WHERE c_mktsegment = '12345678901234567890123456789012' AND c_custkey = o_custkey AND l_orderkey = "
				+ " o_orderkey AND o_orderdate < '30-APR-13' AND l_shipdate > '20-APR-13' "
				+ " GROUP BY l_orderkey, o_orderdate, o_shippriority "
				+ " ORDER BY revenue desc, o_orderdate";

		PreparedStatement preparedStatement = connection.prepareStatement(selectSQL);
		
		return preparedStatement;
	}

	 public PreparedStatement query4(Connection connection) throws SQLException {
		// query4
		String selectSQL = "SELECT n_name, sum(l_extendedprice * (1 - l_discount)) as revenue "
				+ " FROM customer, orders, lineitem, supplier, nation, region "
				+ " WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey AND l_suppkey = "
				+ " s_suppkey AND c_nationkey = s_nationkey AND s_nationkey = n_nationkey AND "
				+ " n_regionkey = r_regionkey AND r_name = '12345678901234567890123456789012' AND o_orderdate >= '30-APR-13' "
				+ " AND o_orderdate < '30-APR-14' "
				+ " GROUP BY n_name "
				+ " ORDER BY revenue desc";

		PreparedStatement preparedStatement = connection.prepareStatement(selectSQL);
		
		return preparedStatement;
	 }
	 
}
