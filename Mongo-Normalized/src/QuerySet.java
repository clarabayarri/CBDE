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
import java.util.regex.Pattern;

import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;



public class QuerySet {

	public void executeQueries( DB database )  {
		System.out.println( "-------- Queries ------" );
		
		Long average = (long) 0;
		List<Long> timeDifferences = new ArrayList<Long>();
		for ( int j = 0; j < 5; ++j ) {
			Long start = System.nanoTime();
			query1( database );
			Long end = System.nanoTime();

			timeDifferences.add( j, end-start );
		}

		System.out.println( "Query 1 took " + timeDifferences + 
				" in nanoseconds --- with minimum " + Collections.min(timeDifferences) );
		average += Collections.min(timeDifferences);
		
		timeDifferences = new ArrayList<Long>();
		for ( int j = 0; j < 5; ++j ) {
			Long start = System.nanoTime();
			query2( database );
			Long end = System.nanoTime();

			timeDifferences.add( j, end-start );
		}

		System.out.println( "Query 2 took " + timeDifferences + 
				" in nanoseconds --- with minimum " + Collections.min(timeDifferences) );
		average += Collections.min( timeDifferences );
		
		timeDifferences = new ArrayList<Long>();
		for ( int j = 0; j < 5; ++j ) {
			Long start = System.nanoTime();
			query3( database );
			Long end = System.nanoTime();

			timeDifferences.add( j, end-start );
		}

		System.out.println( "Query 3 took " + timeDifferences + 
				" in nanoseconds --- with minimum " + Collections.min(timeDifferences) );
		average += Collections.min( timeDifferences );
		
		System.out.println( "\nAverage query time " + average/4 + " in nanoseconds\n" );

	}

	private void query1( DB database ) {
		//els mesos comencen en 0
		Calendar calendar = new GregorianCalendar( 2013,03,30 );

		// create our pipeline operations, first with the $match
		BasicDBObject match = new BasicDBObject( "$match", new BasicDBObject( "L_ShipDate", new BasicDBObject( "$lte", calendar.getTime() ) ) );

		BasicDBObject fields = new BasicDBObject( "L_ReturnFlag", 1 );
		fields.put( "L_LineStatus", 1 );
		fields.put( "L_ShipDate", 1 );
		fields.put( "L_Quantity", 1 );
		fields.put( "L_ExtendedPrice", 1 );
		fields.put( "L_ReturnFlag", 1 );
		fields.put( "L_Tax", 1 );
		fields.put( "L_Discount", 1 );
		fields.put( "_id", 0 );
		BasicDBObject project = new BasicDBObject( "$project", fields );
		
		BasicDBObject sortFields = new BasicDBObject( "L_ReturnFlag", 1 );
		sortFields.put( "L_LineStatus", 1 );

		BasicDBObject sort = new BasicDBObject( "$sort", sortFields );

		DBCollection myColl = database.getCollection( "lineitem" );
		AggregationOutput out = myColl.aggregate( match, project, sort );
		
		Map<String, BasicDBObject> result = new LinkedHashMap<String, BasicDBObject>();
		for (DBObject object : out.results()) {
			String key = object.get( "L_ReturnFlag" ) + " - " + object.get( "L_LineStatus" );
			BasicDBObject group = result.get(key);
			if (group == null) {
				group = new BasicDBObject();
				group.put( "L_ReturnFlag", object.get( "L_ReturnFlag" ) );
				group.put( "L_LineStatus", object.get( "L_LineStatus" ) );
				group.put( "sum_qty", 0 );
				group.put( "sum_base_price", 0 );
				group.put( "sum_disc_price", 0 );
				group.put( "sum_charge", 0 );
				group.put( "count_order", 0 );
				group.put( "sum_discount", 0 );
			}
			int quantity = new Integer( object.get( "L_Quantity" ).toString() );
			int sumQuantity = group.getInt( "sum_qty" ) + quantity;
			group.put("sum_qty", sumQuantity);
			
			double discount = new Double( object.get( "L_Discount" ).toString() );

			double extendedPrice = new Double( object.get( "L_ExtendedPrice" ).toString() );
			double sumExtended = group.getDouble( "sum_base_price" ) + extendedPrice;
			group.put( "sum_base_price", sumExtended );

			double sum_disc_price = group.getDouble( "sum_disc_price" ) + ( extendedPrice*( 1 - discount ) );
			group.put( "sum_disc_price", sum_disc_price );

			double tax = new Double( object.get( "L_Tax" ).toString() );
			double sum_charge = group.getDouble( "sum_charge" ) + ( extendedPrice*( 1 - discount )*( 1 + tax ) );
			group.put( "sum_charge", sum_charge );

			int num_elements = group.getInt( "count_order" ) + 1;
			group.put( "count_order", num_elements );

			double sum_discount = group.getDouble( "sum_discount" ) + discount;
			group.put( "sum_discount", sum_discount );

			result.put( key, group );
		}
		
		for (String key : result.keySet()) {
			BasicDBObject group = result.get(key);
			double average_quantity = group.getDouble( "sum_qty" )/group.getDouble( "count_order" );
			group.put( "avg_qty", average_quantity );
			
			double average_extended = group.getDouble( "sum_base_price" )/group.getDouble( "count_order" );
			group.put( "avg_price", average_extended );
			
			double average_discount = group.getDouble( "sum_discount" )/group.getDouble( "count_order" );
			group.put( "avg_disc", average_discount );
			
			group.remove( "sum_discount" );
		}
		
		
//		BufferedWriter writer = null;
//		try {
//			writer = new BufferedWriter(new FileWriter(
//					"./aggregationOutput.txt"));
//			writer.write(result.toString());
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
	}
	
	private void query2( DB database ) {
		// Part
		Pattern regex = Pattern.compile( "12345678901234567890123456789012" );
		BasicDBObject clause1 = new BasicDBObject( "P_Type", regex );   
		BasicDBObject clause2 = new BasicDBObject( "P_Size", 1000 );
		BasicDBList and = new BasicDBList();
		and.add( clause1 );
		and.add( clause2 );
		DBObject query = new BasicDBObject( "$and", and );
		BasicDBObject match = new BasicDBObject( "$match", query );
		
		BasicDBObject fields = new BasicDBObject();
		fields.put( "_id", 1 );
		fields.put( "P_Mfgr", 1 );
		BasicDBObject project = new BasicDBObject( "$project", fields );
		
		BasicDBObject sortFields = new BasicDBObject( "_id", 1 );
		BasicDBObject sort = new BasicDBObject( "$sort", sortFields );
		
		DBCollection partColl = database.getCollection( "part" );
		AggregationOutput partOut = partColl.aggregate( match, project, sort );
		
//		for ( DBObject part : partOut.results() ) 
//			System.out.println( part );
		
		// Partsupp
		fields = new BasicDBObject( "PS_PartKey", 1 );
		fields.put( "PS_SuppKey", 1 );
		fields.put( "PS_SupplyCost", 1 );
		fields.put( "_id", 0 );
		project = new BasicDBObject( "$project", fields );

		DBCollection partSuppColl = database.getCollection( "partSupp" );
		AggregationOutput partSuppOut = partSuppColl.aggregate( project );
		
//		for ( DBObject partSupp : partSuppOut.results() ) 
//			System.out.println( partSupp );
		
		// Supplier
		fields = new BasicDBObject( "_id", 1 );
		fields.put( "S_Name", 1 );
		fields.put( "S_Address", 1 );
		fields.put( "S_NationKey",  1 );
		fields.put( "S_Phone", 1 );
		fields.put( "S_AcctBal", 1 );
		fields.put( "S_Comment", 1 );
		project = new BasicDBObject( "$project", fields );
		
		sortFields = new BasicDBObject( "S_AcctBal", -1 );
		sortFields.put( "s_name", 1 );

		sort = new BasicDBObject( "$sort", sortFields );

		DBCollection supplierColl = database.getCollection( "supplier" );
		AggregationOutput supplierOut = supplierColl.aggregate( project, sort );
		
//		for ( DBObject supplier : supplierOut.results() ) 
//			System.out.println( supplier );
		
		// Nation
		fields = new BasicDBObject( "_id", 1 );
		fields.put( "N_Name", 1 );
		fields.put( "N_RegionKey", 1 );
		project = new BasicDBObject( "$project", fields );
		
		sortFields = new BasicDBObject( "N_Name", 1 );

		sort = new BasicDBObject( "$sort", sortFields );
		
		DBCollection nationColl = database.getCollection( "nation" );
		AggregationOutput nationOut = nationColl.aggregate( project, sort );
		
//		for ( DBObject nation : nationOut.results() ) 
//			System.out.println( nation );
		
		// Region
		match = new BasicDBObject( "$match", new BasicDBObject( "R_Name", "12345678901234567890123456789012" ) );

		fields = new BasicDBObject( "R_Name", 1 );
		fields.put( "_id", 1 );
		project = new BasicDBObject( "$project", fields );

		DBCollection regionColl = database.getCollection( "region" );
		AggregationOutput regionOut = regionColl.aggregate( match, project );
		
//		for ( DBObject region : regionOut.results() ) 
//			System.out.println( region );
		
		// JOIN
		// ps_supplycost =  SELECT 
		double supplyCostQueried = 0;
		Set<DBObject> partSupps = new HashSet<DBObject>();
		for ( DBObject part : partOut.results() ) {
			supplyCostQueried = getSubquery2( database, new Integer( part.get("_id").toString() ) );
			for ( DBObject partSupp : partSuppOut.results() ) {
				if ( partSupp.get( "PS_SupplyCost" ).equals( supplyCostQueried) ) {
					partSupps.add( partSupp );
				}
			}
		}
		
		// n_regionkey = r_regionkey
		Map<String, DBObject> nations = new LinkedHashMap<String, DBObject>();
		for ( DBObject region : regionOut.results() ) 
			for ( DBObject nation : nationOut.results() ) 
				if ( nation.get( "N_RegionKey" ).equals( region.get( "_id" ) ) ) 
					nations.put( nation.get( "_id" ).toString(), nation );
		
		// s_nationkey = n_nationkey 
		Map<String, DBObject> suppliersWithNation = new LinkedHashMap<String, DBObject>();
		for ( DBObject supplier : supplierOut.results() )
			if (nations.containsKey( supplier.get( "S_NationKey" ).toString() ) )
				suppliersWithNation.put( supplier.get( "_id" ).toString(), supplier );

		//s_suppkey = ps_suppkey & p_partkey = ps_partkey 
		Map<String, DBObject> parts = new LinkedHashMap<String, DBObject>();
		Map<String, DBObject> suppliers = new LinkedHashMap<String, DBObject>();
		Set<DBObject> partSuppsRelation = new HashSet<DBObject>();
		for ( DBObject partSupp : partSupps ) {
			for ( DBObject part : partOut.results() ) {
				if ( part.get( "_id" ).equals( partSupp.get( "PS_PartKey" ) ) ) {
					if ( suppliersWithNation.containsKey( partSupp.get( "PS_SuppKey" ).toString() ) ) {
						parts.put( part.get( "_id" ).toString(), part );
						suppliers.put( partSupp.get( "PS_SuppKey" ).toString(), suppliersWithNation.get( partSupp.get( "PS_SuppKey" ).toString() ) );
						partSuppsRelation.add( partSupp );
					}
				}
			}
		}
		
		DBCollection resultTable = database.getCollection( "resultQuery2" );
		for (DBObject partSupp : partSuppsRelation) {
			int suppKey, partKey, nationKey;
			suppKey = new Integer ( partSupp.get( "PS_SuppKey" ).toString() );
			partKey = new Integer ( partSupp.get( "PS_PartKey" ).toString() );
			nationKey = new Integer ( suppliers.get( Integer.toString( suppKey ) ).get( "S_NationKey" ).toString() );
			
			BasicDBObject rowResult = new BasicDBObject();
			
			rowResult.put( "S_AcctBal", suppliers.get( Integer.toString( suppKey ) ).get( "S_AcctBal" ) );
			rowResult.put( "S_Name", 	suppliers.get( Integer.toString( suppKey ) ).get( "S_Name" ) );
			rowResult.put( "N_Name", 	nations.get( Integer.toString( nationKey ) ).get( "N_Name" ) );
			rowResult.put( "P_PartKey", parts.get( Integer.toString( partKey ) ).get( "_id" ) );
			rowResult.put( "P_Mfgr", 	parts.get( Integer.toString( partKey ) ).get( "P_Mfgr" ) );
			rowResult.put( "S_Address", suppliers.get( Integer.toString( suppKey ) ).get( "S_Address" ) );
			rowResult.put( "S_Phone", 	suppliers.get( Integer.toString( suppKey ) ).get( "S_Phone" ) );
			rowResult.put( "S_Comment", suppliers.get( Integer.toString( suppKey ) ).get( "S_Comment" ) );
			resultTable.insert(rowResult);
		}
		
		// ORDER BY s_acctbal desc, n_name, s_name, p_partkey;
		sortFields = new BasicDBObject( "S_AcctBal", -1 );
		sortFields.put( "N_Name", 1 );
		sortFields.put( "S_Name", 1 );
		sortFields.put( "P_PartKey", 1 );

		sort = new BasicDBObject( "$sort", sortFields );
		
		fields = new BasicDBObject( "_id", 0 );
		fields.put( "S_AcctBal", 1 );
		fields.put( "S_Name", 1 );
		fields.put( "N_Name",  1 );
		fields.put( "P_PartKey", 1 );
		fields.put( "P_Mfgr", 1 );
		fields.put( "S_Address", 1 );
		fields.put( "S_Phone", 1 );
		fields.put( "S_Comment", 1 );
		
		project = new BasicDBObject( "$project", fields );
		
		AggregationOutput resultOut = resultTable.aggregate( project, sort );		
		
		resultTable = database.getCollection( "resultQuery2Sorted" );
		BasicDBObject resultObject = new BasicDBObject();
		resultObject.put( "numResults", partSuppsRelation.size() );
		resultObject.put( "result", resultOut.results() );
		resultTable.insert( resultObject );
		
//		int i = 0;
//		BasicDBObject results = new BasicDBObject();
//		for ( DBObject result : resultOut.results() ) {
//			results.put( Integer.toString(i), result );
//			System.out.println( result );
//			++i;
//		}		
	}
	
	private double getSubquery2( DB database, int partKey ) {
		// Region
		BasicDBObject match = new BasicDBObject( "$match", new BasicDBObject( "R_Name", "12345678901234567890123456789012" ) );

		BasicDBObject fields = new BasicDBObject( "R_Name", 1 );
		fields.put( "_id", 1 );
		BasicDBObject project = new BasicDBObject( "$project", fields );

		DBCollection regionColl = database.getCollection( "region" );
		AggregationOutput regionOut = regionColl.aggregate( match, project );
		
		// Nation
		fields = new BasicDBObject( "N_RegionKey", 1 );
		fields.put( "_id", 1 );
		project = new BasicDBObject( "$project", fields );

		DBCollection nationColl = database.getCollection( "nation" );
		AggregationOutput nationOut = nationColl.aggregate( project );

		
		// Supplier
		fields = new BasicDBObject( "S_NationKey", 1 );
		fields.put( "_id", 1 );
		project = new BasicDBObject( "$project", fields );

		DBCollection supplierColl = database.getCollection( "supplier" );
		AggregationOutput supplierOut = supplierColl.aggregate( project );
		
		
		// PartSupp
		match = new BasicDBObject( "$match", new BasicDBObject( "PS_PartKey", partKey ) );
		
		fields = new BasicDBObject( "PS_SuppKey", 1 );
		fields.put( "PS_SupplyCost", 1 );
		fields.put( "_id", 0 );
		project = new BasicDBObject( "$project", fields );

		DBCollection partSuppColl = database.getCollection( "partSupp" );
		AggregationOutput partSuppOut = partSuppColl.aggregate( match, project );
		
		Set<DBObject> nations = new HashSet<DBObject>();
		for ( DBObject region : regionOut.results() ) {
			for ( DBObject nation : nationOut.results() ) {
				if ( nation.get( "N_RegionKey" ).equals( region.get( "_id" ) ) ) {
					nations.add( nation );
				}
			}
		}
		
		Set<DBObject> suppliers = new HashSet<DBObject>();
		for ( DBObject nation : nations ) {
			for ( DBObject supplier : supplierOut.results() ) {
				if (supplier.get( "S_NationKey" ).equals( nation.get( "_id") ) ) {
					suppliers.add( supplier );
				}
			}
		}
		
		double minimum = Double.MAX_VALUE;
		for ( DBObject supplier : suppliers ) {
			for ( DBObject partSupp : partSuppOut.results() ) {
				if (partSupp.get( "PS_SuppKey" ).equals( supplier.get( "_id") ) ) {
					minimum = Math.min( minimum, new Double( partSupp.get( "PS_SupplyCost" ).toString() ) );
				}
			}
		}
		
		return minimum;
	}

	private void query3( DB database ) {
		// LINEITEM
		// els mesos comencen en 0
		Calendar calendar = new GregorianCalendar( 2013,03,20 );
		BasicDBObject match = new BasicDBObject( "$match", new BasicDBObject( "L_ShipDate", new BasicDBObject( "$gt", calendar.getTime() ) ) );

		BasicDBObject fields = new BasicDBObject( "L_OrderKey", 1 );
		fields.put( "L_ExtendedPrice", 1 );
		fields.put( "L_Discount", 1 );		
		fields.put( "_id", 1 );
		BasicDBObject project = new BasicDBObject( "$project", fields );

		DBCollection lineitemColl = database.getCollection( "lineitem" );
		AggregationOutput lineitemOut = lineitemColl.aggregate( match, project );

		// ORDERS
		calendar = new GregorianCalendar( 2013,03,30 );
		match = new BasicDBObject( "$match", new BasicDBObject( "O_OrderDate", new BasicDBObject( "$lt", calendar.getTime() ) ) );

		fields = new BasicDBObject( "O_OrderDate", 1 );
		fields.put( "O_ShipPriority", 1 );
		fields.put( "O_CustKey", 1 );		
		fields.put( "_id", 1 );
		project = new BasicDBObject( "$project", fields );

		DBCollection ordersColl = database.getCollection( "orders" );
		AggregationOutput ordersOut = ordersColl.aggregate( match, project );
		
		// CUSTOMER
		BasicDBObject clause = new BasicDBObject( "C_MktSegment", "12345678901234567890123456789012" );
		match = new BasicDBObject( "$match", clause );

		fields = new BasicDBObject( "_id", 1 );
//		fields.put( "C_MktSegment", 0 );
		project = new BasicDBObject( "$project", fields );
		
		DBCollection customerColl = database.getCollection( "customer" );
		AggregationOutput customerOut = customerColl.aggregate( match, project );

		// JOINS
		// c_custkey = o_custkey
		Map<String, DBObject> orders = new LinkedHashMap<String, DBObject>();
		for ( DBObject order : ordersOut.results() ) 
			for ( DBObject customer : customerOut.results() ) 
				if ( customer.get( "_id" ).equals( order.get( "O_CustKey" ) ) ) 
					orders.put( order.get( "_id" ).toString(), order );
		
		// l_orderkey = o_orderkey 
		Map<String, DBObject> lineitems = new LinkedHashMap<String, DBObject>();
		Map<String, DBObject> finalOrders = new LinkedHashMap<String, DBObject>();
		for ( DBObject lineitem : lineitemOut.results() ) {
			if ( orders.containsKey( lineitem.get( "L_OrderKey" ).toString() ) ) {
				finalOrders.put( lineitem.get( "L_OrderKey" ).toString() , orders.get( lineitem.get( "L_OrderKey" ).toString() ) );
				lineitems.put( lineitem.get( "_id" ).toString(), lineitem );
			}
		}
				
//		System.out.println( "lineitems --- " +  lineitems.size() );
//		System.out.println( "finalOrders --- " +  finalOrders.size() );
//		Tant a oracle com a mongo amb la segona tanda d'insercions feta la query retorna 361 linies, xaxi nais
		
		DBCollection resultColl = database.getCollection( "resultQuery3Normalized" );
		Set<String> ordersAnalized = new HashSet<String>();		
		
		for ( String orderKey : finalOrders.keySet() ) {
			Set<String> lineitemsAnalized = new HashSet<String>();
			BasicDBObject document = new BasicDBObject();
			double revenue = 0;
			for ( String lineitemKey : lineitems.keySet() ) {
				if ( orderKey.equals( (lineitems.get(lineitemKey)).get( "L_OrderKey" ).toString() ) ) {
					lineitemsAnalized.add(lineitemKey);
					double discount = new Double( ( lineitems.get( lineitemKey ) ).get( "L_Discount" ).toString() );
					double extendedPrice = new Double( ( lineitems.get( lineitemKey ) ).get( "L_ExtendedPrice" ).toString() );
					revenue += extendedPrice*( 1 - discount );
				}
			}
			for ( String lineitemAnalized : lineitemsAnalized ) {
				lineitems.remove(lineitemAnalized);
			}
			
			document.put( "L_OrderKey", orderKey );
			document.put( "revenue", revenue );
			document.put( "O_OrderDate", finalOrders.get( orderKey ).get( "O_OrderDate" ) );
			document.put( "O_ShipPriority", finalOrders.get( orderKey ).get( "O_ShipPriority" ) );
			resultColl.insert( document );
		}
		
		
		
		
		fields = new BasicDBObject( "_id", 0 );
		fields.put( "L_OrderKey", 1 );
		fields.put( "revenue", 1 );
		fields.put( "O_OrderDate", 1 );
		fields.put( "O_ShipPriority", 1);
		
		project = new BasicDBObject( "$project", fields );
		
		BasicDBObject sortFields = new BasicDBObject( "revenue", -1 );
		sortFields.put( "O_OrderDate", 1 );

		BasicDBObject sort = new BasicDBObject( "$sort", sortFields );
		AggregationOutput resultOut = resultColl.aggregate( project, sort );
		
		 BufferedWriter writer = null;
	     try {
	       writer = new BufferedWriter(new FileWriter(
	           "./query3MongoNormalized.txt"));
	       writer.write(resultOut.toString());
	     } catch (IOException e) {
	       e.printStackTrace();
	     }
	
	}
}

