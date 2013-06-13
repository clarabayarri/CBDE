import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
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
		for ( int j = 0; j < 5; ++j ) {
			Long start = System.nanoTime();
			query1( graphDB );
			Long end = System.nanoTime();

			timeDifferences.add( j, end - start );
		}
		
		System.out.println( "Query 1 took " + timeDifferences + 
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
		System.out.println( result.dumpToString() );
		
	}
	
}
