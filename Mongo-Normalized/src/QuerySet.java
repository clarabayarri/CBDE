import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.List;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;




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
//		BasicDBObject query = 
//			database.lineitem.aggregate([
//		        {$match: {l_shipdate: {$lt: '01-MAY-13'}}},
//				{$group: 
//					{_id: {l_returnflag: "$l_returnflag", l_linestatus: "$l_linestatus"},
//					l_returnflag: "$l_returnflag",
//					l_linestatus: "$l_linestatus",
//					sum_qty: {$sum: "$l_quantity"},
//					sum_base_price: {$sum: "$l_extendedprice"},
//					sum_disc_price: {$sum: "$l_extendedprice*(1-$l_discount)"},
//					sum_charge: {$sum: "$l_extendedprice*(1-$l_discount)*(1+$l_tax)"},
//					avg_qty: {$avg: "$l_quantity"},
//					avg_price: {$avg: "$l_extendedprice"},
//					avg_disc: {$avg: "$l_discount"},
//					count_order: {$sum: 1}
//					}},
//				{$sort: {l_returnflag: 1, l_linestatus: 1}}
//			]);
		
		BasicDBObject cmdBody = new BasicDBObject("aggregate", "lineitem");
		DBCollection myColl = database.getCollection("lineitem");
		ArrayList<BasicDBObject> pipeline = new ArrayList<BasicDBObject>(); 
		
		Calendar calendar = new GregorianCalendar(2013,5,1);
		
		// create our pipeline operations, first with the $match
		BasicDBObject match = new BasicDBObject("$match", new BasicDBObject("l_shipdate", new BasicDBObject("$lt", new java.sql.Date(calendar.getTime().getTime()))));

		// Now the $group operation
		BasicDBObject groupFields = new BasicDBObject( "l_returnflag", "$l_returnflag" );
		groupFields.put( "l_linestatus", "$l_linestatus" );
		groupFields = new BasicDBObject( "_id", groupFields);
		
//		groupFields.put("l_returnflag", "$l_returnflag");
//		groupFields.put("l_linestatus", "$l_linestatus");
		
		groupFields.put("sum_qty", new BasicDBObject( "$sum", "$l_quantity"));
		groupFields.put("sum_base_price", new BasicDBObject( "$sum", "$l_extendedprice"));
		groupFields.put("sum_disc_price", new BasicDBObject( "$sum", "$l_extendedprice*(1-$l_discount)"));
		groupFields.put("sum_charge", new BasicDBObject( "$sum", "$l_extendedprice*(1-$l_discount)*(1+$l_tax)"));
		
		groupFields.put("avg_qty", new BasicDBObject( "$avg", "$l_quantity"));
		groupFields.put("avg_price", new BasicDBObject( "$avg", "$l_extendedprice"));
		groupFields.put("avg_disc", new BasicDBObject( "$avg", "$l_discount"));
		
		groupFields.put("count_order", new BasicDBObject( "$sum", 1));
		
		BasicDBObject group = new BasicDBObject("$group", groupFields);
		
		//in sorting 1 is ASC -1 is DESC
		BasicDBObject sortFields = new BasicDBObject( "l_returnflag", 1 );
		sortFields.put( "l_linestatus", 1 );
		
		BasicDBObject sort = new BasicDBObject( "$sort", sortFields);
		
		pipeline.add(match);
	    pipeline.add(group);
	    pipeline.add(sort);
	
	    cmdBody.put("pipeline", pipeline);
	    
		// run aggregation
		System.out.println(myColl.aggregate(match, group, sort));
		
		return cmdBody;
	}
		
}

