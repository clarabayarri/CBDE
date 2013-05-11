import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.List;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;



public class QuerySet {

	public void executeQueries(DB database)  {
		System.out.println("-------- Queries ------");
		List<BasicDBObject> queries = new ArrayList<BasicDBObject>();
		queries.add(query1(database));
		
		Long average = (long) 0;
		for (int i = 0; i < queries.size(); ++i) {
			List<Long> timeDifferences = new ArrayList<Long>();
			for (int j = 0; j < 5; ++j) {
				Long start = System.nanoTime();
				database.command(queries.get(i));
				Long end = System.nanoTime();

				timeDifferences.add(j, end-start);
			}
			
			System.out.println("Query" + (i + 1) + " took " + timeDifferences + 
					" in nanoseconds --- with minimum " + Collections.min(timeDifferences));
			average += Collections.min(timeDifferences);
		}
		System.out.println("\nAverage query time " + average/queries.size() + " in nanoseconds\n");
		
	}

	private BasicDBObject query1(DB database) {
		BasicDBObject cmdBody = new BasicDBObject("aggregate", "lineitem");
		ArrayList<BasicDBObject> pipeline = new ArrayList<BasicDBObject>(); 
		//els mesos comencen en 0
		Calendar calendar = new GregorianCalendar(2013,03,30);
		
		// create our pipeline operations, first with the $match
		BasicDBObject match = new BasicDBObject("$match", new BasicDBObject("L_ShipDate", new BasicDBObject("$lte", calendar.getTime())));

		// Now the $group operation
		BasicDBObject groupFields = new BasicDBObject( "L_ReturnFlag", "$L_ReturnFlag" );
		groupFields.put( "L_LineStatus", "$L_LineStatus" );
		groupFields.put("L_ShipDate", "$L_ShipDate");
		groupFields = new BasicDBObject( "_id", groupFields);
				
		groupFields.put("sum_qty", new BasicDBObject( "$sum", "$L_Quantity"));
		groupFields.put("sum_base_price", new BasicDBObject( "$sum", "$L_ExtendedPrice"));
//		--------------------------------------------------------------------------------------------------------------------
//		groupFields.put("sum_disc_price", new BasicDBObject( "$sum", "$L_ExtendedPrice*(1-$L_Discount)"));
//		groupFields.put("sum_charge", new BasicDBObject( "$sum", "$L_ExtendedPrice*(1-$L_Discount)*(1+$L_Tax)"));
//		--------------------------------------------------------------------------------------------------------------------
		groupFields.put("avg_qty", new BasicDBObject( "$avg", "$L_Quantity"));
		groupFields.put("avg_price", new BasicDBObject( "$avg", "$L_ExtendedPrice"));
		groupFields.put("avg_disc", new BasicDBObject( "$avg", "$L_Discount"));
		
		groupFields.put("count_order", new BasicDBObject( "$sum", 1));

		BasicDBObject group = new BasicDBObject("$group", groupFields);
		
		//in sorting 1 is ASC -1 is DESC
		BasicDBObject sortFields = new BasicDBObject( "_id", 1 );
		
		BasicDBObject sort = new BasicDBObject( "$sort", sortFields);
		
		pipeline.add(match);
	    pipeline.add(group);
	    pipeline.add(sort);
	
	    cmdBody.put("pipeline", pipeline);
//	    DBCollection myColl = database.getCollection("lineitem");
//	    AggregationOutput out = myColl.aggregate(match, group, sort);
//		BufferedWriter writer = null;
//		try {
//			writer = new BufferedWriter(new FileWriter(
//					"./aggregationOutput.txt"));
//			writer.write(out.toString());
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
		
		return cmdBody;
	}
		
}

