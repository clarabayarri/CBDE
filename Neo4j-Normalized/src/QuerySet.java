import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.GraphDatabaseService;



public class QuerySet {
	
	public void executeQueries( GraphDatabaseService graphDB ) {
		System.out.println("-------- Queries ------");

		Long average = (long) 0;
		List<Long> timeDifferences = new ArrayList<Long>();
		
		// Query 1
		for ( int j = 0; j < 5; ++j ) {
			Long start = System.nanoTime();
			query1( graphDB );
			Long end = System.nanoTime();

			timeDifferences.add( j, end - start );
		}
		
		System.out.println( "Query 1 took " + timeDifferences + 
				" in nanoseconds --- with minimum " + Collections.min( timeDifferences ) );
		average += Collections.min(timeDifferences);
		
		// Query 3
		for ( int j = 0; j < 5; ++j ) {
			Long start = System.nanoTime();
			query3( graphDB );
			Long end = System.nanoTime();

			timeDifferences.add( j, end - start );
		}
		
		System.out.println( "Query 3 took " + timeDifferences + 
				" in nanoseconds --- with minimum " + Collections.min( timeDifferences ) );
		average += Collections.min(timeDifferences);
		
		//Query 4
		for ( int j = 0; j < 5; ++j ) {
			Long start = System.nanoTime();
			query4( graphDB );
			Long end = System.nanoTime();

			timeDifferences.add( j, end - start );
		}
		
		System.out.println( "Query 4 took " + timeDifferences + 
				" in nanoseconds --- with minimum " + Collections.min( timeDifferences ) );
		average += Collections.min(timeDifferences);
		
		System.out.println( "\nAverage query time " + average/4 + " in nanoseconds\n" );
	}
	
	private void query1( GraphDatabaseService graphDB ) {

		Calendar calendar = new GregorianCalendar(2013,3,30);
		ExecutionEngine engine = new ExecutionEngine( graphDB );
		ExecutionResult result = engine.execute( 
				"START lineitem=node( * ) " +
				"WHERE HAS ( lineitem.L_ShipDate ) AND HAS ( lineitem.L_Quantity ) AND HAS ( lineitem.L_ExtendedPrice ) " +
						"AND HAS ( lineitem.L_Discount ) AND HAS ( lineitem.L_Tax ) " +
						"AND lineitem.L_ShipDate <= " + calendar.getTime().getTime() +
				"RETURN lineitem.L_ReturnFlag, lineitem.L_LineStatus, SUM( lineitem.L_Quantity ) AS sum_qty, " +
					"SUM( lineitem.L_ExtendedPrice ) AS sum_base_price, " +
					"SUM( lineitem.L_ExtendedPrice*( 1 - lineitem.L_Discount) ) AS sum_disc_price, " +
					"SUM( lineitem.L_ExtendedPrice*( 1 - lineitem.L_Discount )*( 1 + lineitem.L_Tax ) ) AS sum_charge, " +
					"AVG( lineitem.L_Quantity ) AS avg_qty, AVG( lineitem.L_ExtendedPrice) AS avg_price, " +
					"AVG( lineitem.L_Discount) AS avg_disc, count(*) AS count_order " +
				"ORDER BY lineitem.L_ReturnFlag, lineitem.L_LineStatus"
				);
		//System.out.println( result.dumpToString() );
		
	}
	
	private void query3( GraphDatabaseService graphDB ) {

		Calendar calendar = new GregorianCalendar(2013,3,29);
		Calendar calendar2 = new GregorianCalendar(2013,3,30);
		ExecutionEngine engine = new ExecutionEngine( graphDB );
		ExecutionResult result = engine.execute( 
				"START customer = node(*) " +
				"MATCH (customer)-[:" + DataInserter.RelTypes.HAS_ORDER.name() + "]->(orders)-[:" + DataInserter.RelTypes.HAS_LINEITEM.name() + "]->(lineitem)" +
				"WHERE (has(customer.C_MktSegment)) and (customer.C_MktSegment = '12345678901234567890123456789012') and " +
						"(has(orders.O_OrderDate)) and (orders.O_OrderDate < " + calendar.getTime().getTime() + ") and" + 
						"(has(lineitem.L_ShipDate)) and (lineitem.L_ShipDate > " + calendar2.getTime().getTime() + ")" + 
				"RETURN orders.O_OrderKey, sum(lineitem.L_ExtendedPrice * (1-lineitem.L_Discount)) as revenue, orders.O_OrderDate, orders.O_ShipPriority " +
				"ORDER BY revenue DESC, orders.O_OrderDate"
				);
		//System.out.println( result.dumpToString() );
		// 191 rows
	}
	
	private void query4( GraphDatabaseService graphDB ) {

		Calendar calendar = new GregorianCalendar(2013,3,29);
		Calendar calendar2 = new GregorianCalendar(2014,3,30);
		ExecutionEngine engine = new ExecutionEngine( graphDB );
		ExecutionResult result = engine.execute( 
				"START region = node(*) " +
				"MATCH (region)-[:" + DataInserter.RelTypes.HAS_NATION.name() + "]->(nation)-[:" + DataInserter.RelTypes.HAS_SUPPLIER.name() + "]->(supplier)" +
					"-[:" + DataInserter.RelTypes.SUPPLIER_HAS_PARTSUPP.name() + "]->(partsupp)-[:" + DataInserter.RelTypes.PARTSUPP_HAS_LINEITEM.name() + "]->(lineitem), " +
					"(nation)-[:" + DataInserter.RelTypes.HAS_CUSTOMER.name() + "]->(customer)-[:" + DataInserter.RelTypes.HAS_ORDER.name() + "]->(orders)-[:" + DataInserter.RelTypes.HAS_LINEITEM.name() + "]->(lineitem)" +
				"WHERE (has(region.R_Name)) and (region.R_Name = '12345678901234567890123456789012') and " +
						"(has(orders.O_OrderDate)) and (orders.O_OrderDate >= " + calendar.getTime().getTime() + ") and" + 
						"(orders.O_OrderDate < " + calendar2.getTime().getTime() + ")" + 
				"RETURN nation.N_Name, sum(lineitem.L_ExtendedPrice * (1-lineitem.L_Discount)) as revenue " +
				"ORDER BY revenue DESC"
				);
		//System.out.println( result.dumpToString() );
		// 3 rows
	}
	
}
