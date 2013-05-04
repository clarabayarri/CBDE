import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;


public class DataInserter {

	private Random random = new Random(3l);
	
	private float SF = 0.00333333f;
	
	private List<Integer> regionIds = new ArrayList<Integer>();
	private List<Integer> nationIds = new ArrayList<Integer>();
	private List<Integer> partIds = new ArrayList<Integer>();
	private List<Integer> supplierIds = new ArrayList<Integer>();
	private Map<Integer, List<Integer>> partSuppIds = new HashMap<Integer, List<Integer>>();
	private List<Integer> customerIds = new ArrayList<Integer>();
	private List<Integer> orderIds = new ArrayList<Integer>();
	private Map<Integer, Integer> lineItemIds = new HashMap<Integer, Integer>();
	
	public void initialInsert(DB database) {
		System.out.println("-------- Initial insertion ------");
		
		Map<DBCollection, List<BasicDBObject>> inserts = new HashMap<DBCollection, List<BasicDBObject>>();
		inserts.put(database.getCollection("region"), getRegionObjects());
		
		Date startDate = new Date();
		for (DBCollection collection : inserts.keySet()) {
			List<BasicDBObject> individualInserts = inserts.get(collection);
			for (BasicDBObject object : individualInserts) {
				collection.insert(object);
			}
		}
		Date endDate = new Date();
		
		Long timeDifference = endDate.getTime() - startDate.getTime();
		System.out.println("Insertion took " + timeDifference + " milliseconds.\n");
	}
	
	public void secondInsert(DB database) {
		
	}
	
	private Integer getRandomInteger() {
		// int must have 4 digits
		return random.nextInt(100000 - 1000) + 1000;
	}
	
	private double getRandomDouble(int x) {
		double sum = random.nextInt(9) + 1;
		for (int i = 1; i < x/2; ++i) {
			sum *= 10;
			sum += random.nextInt(10);
		}
		return sum;
	}
	
	private String getRandomString(int size) {
		String result = "";
		for (int i = 0; i < size/2; ++i) {
			int number = random.nextInt(20);
			char chara = (char) ('a' + number);
			result += chara;
		}
		return result;
	}
	
	private Date getRandomDate() {
		Calendar calendar = new GregorianCalendar();
		calendar.setTime(new Date());
		calendar.add(Calendar.DAY_OF_YEAR, random.nextInt(10000)-5000);
		return calendar.getTime();
	}
	
	public List<BasicDBObject> getRegionObjects() {
		// R_RegionKey, R_Name, R_Comment, skip
		List<BasicDBObject> objects = new ArrayList<BasicDBObject>();
		for (int i = 1; i <= 5; ++i) {
			BasicDBObject document = new BasicDBObject();
			
			Integer id = getRandomInteger();
			while(regionIds.contains(id)) id = getRandomInteger();
			regionIds.add(id);
			document.put("_id", id);
			if (i == 1) 
				document.put("R_Name", "12345678901234567890123456789012");
			else
				document.put("R_Name", getRandomString(64));
			document.put("R_Comment", getRandomString(160));
			document.put("skip", getRandomString(64));
			objects.add(document);
		}
		return objects;
	}
}
