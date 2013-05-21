import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class QuerySet {

	public void executeQueries(DB database)  {
		System.out.println("-------- Queries ------");
		
		Long average = (long) 0;
		List<Long> timeDifferences = new ArrayList<Long>();
		for (int j = 0; j < 5; ++j) {
			timeDifferences.add(j, query1(database));
		}

		System.out.println("Query 1 took " + timeDifferences + 
				" in nanoseconds --- with minimum " + Collections.min(timeDifferences));
		average += Collections.min(timeDifferences);
		
		timeDifferences = new ArrayList<Long>();
		for (int j = 0; j < 5; ++j) {
			timeDifferences.add(j, query2(database));
		}

		System.out.println("Query 2 took " + timeDifferences + 
				" in nanoseconds --- with minimum " + Collections.min(timeDifferences));
		average += Collections.min(timeDifferences);
		
		
		System.out.println("\nAverage query time " + average/4 + " in nanoseconds\n");

	}

	private Long query1(DB database) {
		Long start = System.nanoTime();
		
		//els mesos comencen en 0
		Calendar calendar = new GregorianCalendar(2013,03,30);

		BasicDBObject match = new BasicDBObject("$match", new BasicDBObject("L_ShipDate", new BasicDBObject("$lte", calendar.getTime())));

		BasicDBObject fields = new BasicDBObject("L_ReturnFlag", 1);
		fields.put("L_LineStatus", 1);
		fields.put("L_ShipDate", 1);
		fields.put("L_Quantity", 1);
		fields.put("L_ExtendedPrice", 1);
		fields.put("L_ReturnFlag", 1);
		fields.put("L_Tax", 1);
		fields.put("L_Discount", 1);
		fields.put("_id", 0);
		BasicDBObject project = new BasicDBObject("$project", fields );
		
		BasicDBObject sortFields = new BasicDBObject( "L_ReturnFlag", 1 );
		sortFields.put("L_LineStatus", 1 );

		BasicDBObject sort = new BasicDBObject( "$sort", sortFields);

		DBCollection myColl = database.getCollection("lineitem");
		AggregationOutput out = myColl.aggregate(match, project, sort);
		
		Map<String, BasicDBObject> result = new LinkedHashMap<String, BasicDBObject>();
		for (DBObject object : out.results()) {
			String key = object.get("L_ReturnFlag") + " - " + object.get("L_LineStatus");
			BasicDBObject group = result.get(key);
			if (group == null) {
				group = new BasicDBObject();
				group.put("L_ReturnFlag", object.get("L_ReturnFlag"));
				group.put("L_LineStatus", object.get("L_LineStatus"));
				group.put("sum_qty", 0);
				group.put("sum_base_price", 0);
				group.put("sum_disc_price", 0);
				group.put("sum_charge", 0);
				group.put("count_order", 0);
				group.put("sum_discount", 0);
			}
			int discount = new Integer(object.get("L_Quantity").toString());
			int sumQuantity = group.getInt("sum_qty") + discount;
			group.put("sum_qty", sumQuantity);
			
			double extendedPrice = new Double(object.get("L_ExtendedPrice").toString());
			double sumExtended = group.getDouble("sum_base_price") + extendedPrice;
			group.put("sum_base_price", sumExtended);
			
			double sum_disc_price = group.getDouble("sum_disc_price") + (extendedPrice * (1-discount));
			group.put("sum_disc_price", sum_disc_price);
			
			double tax = new Double(object.get("L_Tax").toString());
			double sum_charge = group.getDouble("sum_charge") + (extendedPrice * (1-discount) * (1 + tax));
			group.put("sum_charge", sum_charge);
			
			int num_elements = group.getInt("count_order") + 1;
			group.put("count_order", num_elements);
			
			int sum_discount = group.getInt("sum_discount") + discount;
			group.put("sum_discount", sum_discount);
			
			result.put(key, group);
		}
		
		for (String key : result.keySet()) {
			BasicDBObject group = result.get(key);
			double average_quantity = group.getDouble("sum_qty") / group.getDouble("count_order");
			group.put("avg_qty", average_quantity);
			
			double average_extended = group.getDouble("sum_base_price") / group.getDouble("count_order");
			group.put("avg_price", average_extended);
			
			double average_discount = group.getDouble("sum_discount") / group.getDouble("count_order");
			group.put("avg_disc", average_discount);
			
			group.remove("sum_discount");
		}
		Long end = System.nanoTime();
		
		BufferedWriter writer = null;
		try {
			writer = new BufferedWriter(new FileWriter(
					"./query1Output.txt"));
			writer.write(out.toString());
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return end-start;
	}
	
	private Long query2(DB database) {
		Long start = System.nanoTime();
		
		double result = getSubquery2(database, 1);
		
		Long end = System.nanoTime();
		return end-start;
	}
	
	private double getSubquery2(DB database, int partKey) {
		// Region
		BasicDBObject match = new BasicDBObject("$match", new BasicDBObject("R_Name", "12345678901234567890123456789012"));

		BasicDBObject fields = new BasicDBObject("R_Name", 1);
		fields.put("_id", 1);
		BasicDBObject project = new BasicDBObject("$project", fields );

		DBCollection regionColl = database.getCollection("region");
		AggregationOutput regionOut = regionColl.aggregate(match, project);
		
		// Nation
		fields = new BasicDBObject("N_RegionKey", 1);
		fields.put("_id", 1);
		project = new BasicDBObject("$project", fields );

		DBCollection nationColl = database.getCollection("nation");
		AggregationOutput nationOut = nationColl.aggregate(project);

		
		// Supplier
		fields = new BasicDBObject("S_NationKey", 1);
		fields.put("_id", 1);
		project = new BasicDBObject("$project", fields );

		DBCollection supplierColl = database.getCollection("supplier");
		AggregationOutput supplierOut = supplierColl.aggregate(project);
		
		
		// PartSupp
		match = new BasicDBObject("$match", new BasicDBObject("PS_PartKey", partKey));
		
		fields = new BasicDBObject("PS_SuppKey", 1);
		fields.put("PS_SupplyCost", 1);
		fields.put("_id", 0);
		project = new BasicDBObject("$project", fields );

		DBCollection partSuppColl = database.getCollection("partSupp");
		AggregationOutput partSuppOut = partSuppColl.aggregate(match, project);
		
		Set<DBObject> nations = new HashSet<DBObject>();
		for (DBObject region : regionOut.results()) {
			for (DBObject nation : nationOut.results()) {
				if (nation.get("N_RegionKey").equals(region.get("_id"))) {
					nations.add(nation);
				}
			}
		}
		
		Set<DBObject> suppliers = new HashSet<DBObject>();
		for (DBObject nation : nations) {
			for (DBObject supplier : supplierOut.results()) {
				if (supplier.get("S_NationKey").equals(nation.get("_id"))) {
					suppliers.add(supplier);
				}
			}
		}
		
		double minimum = Double.MAX_VALUE;
		for (DBObject supplier : suppliers) {
			for (DBObject partSupp : partSuppOut.results()) {
				if (partSupp.get("PS_SuppKey").equals(supplier.get("_id"))) {
					minimum = Math.min(minimum, new Double(partSupp.get("PS_SupplyCost").toString()));
				}
			}
		}
		
		return minimum;
	}

}

