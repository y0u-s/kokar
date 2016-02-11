//package souk.maventeset;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections4.BidiMap;
import org.apache.commons.collections4.bidimap.TreeBidiMap;

import com.google.common.collect.Lists;
import com.mongodb.AggregationOptions;
import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.Cursor;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.util.JSON;
import com.google.common.collect.Lists;

public class Wagenparkbeheer {
	AggregationOutput output;
	MongoClient mongoClient = new MongoClient(new ServerAddress("localhost", 27017));
	DB db = mongoClient.getDB("project");
	DBCollection coll = db.getCollection("positions");
	double dX;
	double dY;
	double SomN;
	double SomE;
	double Latitude;
	double Longitude;
	double x2;
	double y2;
	ArrayList<String> UnitIds = new ArrayList<String>();
	ArrayList<Double> topSpeed = new ArrayList<Double>();
	DBCollection collUnits = db.getCollection("collUnits");
	DBCollection collAan = db.getCollection("collAan");
	DBCollection collTotaal = db.getCollection("collTotaal");
	DBCollection collTotaalAanUit = db.getCollection("collTotaalAanUit");
	DBCollection com = db.getCollection("com");
	DBCollection top3 = db.getCollection("top3");
	HashMap<String, Integer> comHash = new HashMap<String, Integer>();
	BidiMap<Integer, String> bidi = new TreeBidiMap<Integer, String>();
	int val = 0;
	
	public void getData() {

		DBCollection coll = db.getCollection("events");
		DBObject match = new BasicDBObject("$match", new BasicDBObject("Value", "1"));
		DBObject group = new BasicDBObject("$group",
				new BasicDBObject("_id", "$UnitId").append("count", new BasicDBObject("$sum", 1)));
		DBObject sort = new BasicDBObject("$sort", new BasicDBObject("count", -1));
		output = coll.aggregate(match, group, sort);

		for (DBObject result : output.results()) {
			val++;
			System.out.println("UnitId: " + result.get("_id") + ", Aantal: " + result.get("count"));
		}

	}

	public void convert() {

		dX = (double) ((x2 - 155000) * Math.pow(10, -5));
		dY = (double) ((y2 - 463000) * Math.pow(10, -5));

		SomN = (3235.65389 * dY) + (-32.58297 * Math.pow(dX, 2)) + (-0.2475 * Math.pow(dY, 2))
				+ (-0.84978 * Math.pow(dX, 2) * dY) + (-0.0655 * Math.pow(dY, 3))
				+ (-0.01709 * Math.pow(dX, 2) * Math.pow(dY, 2)) + (-0.00738 * dX) + (0.0053 * Math.pow(dX, 4))
				+ (-0.00039 * Math.pow(dX, 2) * Math.pow(dY, 3)) + (0.00033 * Math.pow(dX, 4) * dY)
				+ (-0.00012 * dX * dY);
		SomE = (5260.52916 * dX) + (105.94684 * dX * dY) + (2.45656 * dX * Math.pow(dY, 2))
				+ (-0.81885 * Math.pow(dX, 3)) + (0.05594 * dX * Math.pow(dY, 3)) + (-0.05607 * Math.pow(dX, 3) * dY)
				+ (0.01199 * dY) + (-0.00256 * Math.pow(dX, 3) * Math.pow(dY, 2)) + (0.00128 * dX * Math.pow(dY, 4))
				+ (0.00022 * Math.pow(dY, 2)) + (-0.00022 * Math.pow(dX, 2)) + (0.00026 * Math.pow(dX, 4));

		Latitude = 52.15517 + (SomN / 3600);
		Longitude = 5.387206 + (SomE / 3600);
	}

	public void positions() {

		DBObject group = new BasicDBObject("$group",
				new BasicDBObject("_id", "$UnitId").append("count", new BasicDBObject("$sum", 1)));

		output = coll.aggregate(group);

		for (DBObject result : output.results()) {
			UnitIds.add(result.get("_id").toString());
			String unit = new String();
			unit = result.get("_id").toString();
			DBCollection coll1 = db.getCollection("positions");

			BasicDBObject whereQuery = new BasicDBObject();
			whereQuery.put("UnitId", unit);
			DBCursor cursor = coll1.find(whereQuery);
			int i = 0;
			double gereden = 0;
			double l1 = 0;
			double l2 = 0;
			double l3 = 0;
			double l4 = 0;
			while (cursor.hasNext()) {

				BasicDBObject obj = (BasicDBObject) cursor.next();
				ArrayList<String> x = new ArrayList<String>();
				ArrayList<String> y = new ArrayList<String>();
				x.add(obj.getString("Rdx"));
				y.add(obj.getString("Rdy"));
				String y1 = y.get(0);
				String x1 = x.get(0);

				x2 = Double.parseDouble(x1);
				y2 = Double.parseDouble(y1);
				convert();
				i++;
				if (i == 3) {
					i = 1;
				}
				if (i == 1) {
					l1 = Latitude;
					l2 = Longitude;
				} else if (i == 2) {
					l3 = Latitude;
					l4 = Longitude;
				}
				if (distance(l1, l2, l3, l4, "K") < 100) {
					gereden = gereden + distance(l1, l2, l3, l4, "K");

				}

			}
			System.out.println("UnitId: " + unit + ", KM: " + Math.round(gereden));
		}
	}

	public static double distance(double lat1, double lon1, double lat2, double lon2, String unit) {

		double theta = lon1 - lon2; // of omgekeerd
		double dist = Math.sin(deg2rad(lat1)) * Math.sin(deg2rad(lat2))
				+ Math.cos(deg2rad(lat1)) * Math.cos(deg2rad(lat2)) * Math.cos(deg2rad(theta));
		dist = Math.acos(dist);
		dist = rad2deg(dist);
		dist = dist * 60 * 1.1515;
		if (unit == "K") {
			dist = dist * 1.609344;
		} else if (unit == "N") {
			dist = dist * 0.8684;
		}
		return (dist);
	}

	private static double deg2rad(double deg) {
		return (deg * Math.PI / 180.0);
	}

	private static double rad2deg(double rad) {
		return (rad * 180 / Math.PI);
	}

	public void test() {
		//collUnits.drop();
		collTotaal.drop();
		System.out.println("Time start:" + System.currentTimeMillis());
		long start = System.currentTimeMillis();
		DBCollection coll1 = db.getCollection("positions");
		ArrayList<String> arraylist = new ArrayList<String>();
		Set<String> hs = new HashSet<String>();

		DBObject group = new BasicDBObject("$group",
				new BasicDBObject("_id", "$UnitId").append("count", new BasicDBObject("$sum", 1)));

		output = coll.aggregate(group);
		int k = 0;

		for (DBObject result : output.results()) {
			UnitIds.add(result.get("_id").toString());
			String unit = new String();
			unit = result.get("_id").toString();

			BasicDBObject whereQuery = new BasicDBObject();
			whereQuery.put("UnitId", unit);
			DBCursor cursor = coll1.find(whereQuery);

			while (cursor.hasNext()) {
				DateTimeFormatter formatter1 = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
				String string1 = cursor.next().get("DateTime").toString();
				LocalDate localdate11 = LocalDate.parse(string1, formatter1);
				String lstring11 = localdate11.toString();
				arraylist.add(lstring11.toString());
			}
			hs.addAll(arraylist);
			arraylist.clear();
			arraylist.addAll(hs);
			Collections.sort(arraylist);
			// System.out.println("Dagen zijn in de ArrayList");

			double geredenTotaal = 0;
			for (int m = 0; m < arraylist.size(); m++) {
				BasicDBObject andQuery = new BasicDBObject();
				List<BasicDBObject> obj = new ArrayList<BasicDBObject>();
				obj.add(new BasicDBObject("UnitId", unit));
				obj.add(new BasicDBObject("DateTime", java.util.regex.Pattern.compile(arraylist.get(m))));
				andQuery.put("$and", obj);
				DBCursor cursor1 = coll1.find(andQuery);
				int i = 0;
				double gereden = 0;
				double l1 = 0;
				double l2 = 0;
				double l3 = 0;
				double l4 = 0;
				while (cursor1.hasNext()) {

					BasicDBObject obj1 = (BasicDBObject) cursor1.next();
					ArrayList<String> x = new ArrayList<String>();
					ArrayList<String> y = new ArrayList<String>();
					x.add(obj1.getString("Rdx"));
					y.add(obj1.getString("Rdy"));
					String y1 = y.get(0);
					String x1 = x.get(0);

					x2 = Double.parseDouble(x1);
					y2 = Double.parseDouble(y1);
					convert();
					i++;
					if (i == 3) {
						i = 1;
					}
					if (i == 1) {
						l1 = Latitude;
						l2 = Longitude;
					} else if (i == 2) {
						l3 = Latitude;
						l4 = Longitude;
					}
					if (distance(l1, l2, l3, l4, "K") < 100) {
						gereden = gereden + distance(l1, l2, l3, l4, "K");
						geredenTotaal = geredenTotaal + distance(l1, l2, l3, l4, "K");

					}

				}

				// gson - export naar mongodb
//				String json = "{\"UnitId\": " + "\"" + unit + "\"" + ",\"Dag\": " + "\"" + arraylist.get(m) + "\""
//						+ ", \"KM\": " + "\"" + Math.round(gereden) + "\"}";
//
//				DBObject dbObject = (DBObject) JSON.parse(json);
//				collUnits.insert(dbObject);

				// System.out.println("{\"UnitId\": " + "\"" + unit + "\"" + ",
				// \"Dag\": " + "\"" + arraylist.get(m) + "\""
				// + ", \"KM\": " + "\"" + Math.round(gereden) + "\"}");

			}
			String json = "{\"UnitId\": " + "\"" + unit + "\"" + ",\"Vanaf\": " + "\"" + arraylist.get(0) + "\""
					+ ", \"KM\": " + Math.round(geredenTotaal) + "}";
			System.out.println(json);
//
			DBObject dbObject = (DBObject) JSON.parse(json);
			collTotaal.insert(dbObject);
//			
//			System.out.println(
//					"UnitId: " + unit + ", Vanaf " + arraylist.get(0) + ": " + Math.round(geredenTotaal));
//			System.out.println(
//					"UnitId: " + unit + ", Gemiddeld KM per dag: " + Math.round(geredenTotaal / arraylist.size()));

		}
		System.out.println("End" + System.currentTimeMillis());
		long end = System.currentTimeMillis();
		long diff = end - start;
		System.out.println("Diff" + diff);

	}
	int counter;
	public void ignition() {
		
		collAan.drop();
		DBCollection coll1 = db.getCollection("events");
		ArrayList<String> arraylist = new ArrayList<String>();
		Set<String> hs = new HashSet<String>();

		DBObject group = new BasicDBObject("$group",
				new BasicDBObject("_id", "$UnitId").append("count", new BasicDBObject("$sum", 1)));

		output = coll.aggregate(group);

		for (DBObject result : output.results()) {
			counter++;
			UnitIds.add(result.get("_id").toString());
			String unit = new String();
			unit = result.get("_id").toString();

			BasicDBObject whereQuery = new BasicDBObject();
			whereQuery.put("UnitId", unit);
			DBCursor cursor = coll1.find(whereQuery);

			while (cursor.hasNext()) {
				DateTimeFormatter formatter1 = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
				String string1 = cursor.next().get("DateTime").toString();
				LocalDate localdate11 = LocalDate.parse(string1, formatter1);
				String lstring11 = localdate11.toString();
				arraylist.add(lstring11.toString());
			}
			hs.addAll(arraylist);
			arraylist.clear();
			arraylist.addAll(hs);
			Collections.sort(arraylist);
			// System.out.println("Dagen zijn in de ArrayList");

			for (int m = 0; m < arraylist.size(); m++) {
				BasicDBObject andQuery = new BasicDBObject();
				List<BasicDBObject> obj = new ArrayList<BasicDBObject>();
				obj.add(new BasicDBObject("UnitId", unit));
				obj.add(new BasicDBObject("DateTime", java.util.regex.Pattern.compile(arraylist.get(m))));
				obj.add(new BasicDBObject("Value", "1"));
				andQuery.put("$and", obj);
				DBCursor cursor1 = coll1.find(andQuery);
				int aantal = 0;
				while (cursor1.hasNext()) {
					// System.out.println("UnitId: "+unit+", "+"Dag:
					// "+arraylist.get(m)+", "+ "Aantal keer aan: "+aantal);
					System.out.println(cursor1.next());
					aantal++;
				
				
					//System.out.println(aantal);
				}
				String json = "{\"UnitId\": " + "\"" + unit + "\"" + ",\"Dag\": " + "\"" + arraylist.get(m) + "\""
						+ ", \"Aan\": " + "\"" + aantal + "\"}";
				System.out.println(json);
			
				DBObject dbObject = (DBObject) JSON.parse(json);
				collAan.insert(dbObject);
				System.out.println(counter);
				//System.out.println("UnitId: " + unit + ", " + "Dag: " + arraylist.get(m) + ", " + aantal);

			}
		}
	}

	public void saveData() {

		collTotaalAanUit.drop();
		Iterator<DBObject> mc = output.results().iterator();
		Iterator<DBObject> iteratorToArray = mc;
		List<DBObject> convertedIterator = Lists.newArrayList(iteratorToArray);
		DBObject options = BasicDBObjectBuilder.start().add("capped", true).add("size", 2000000000l).get();
		DBCollection collection = db.createCollection("collTotaalAanUit", options);
		collection.insert(convertedIterator);
	}
	
	public void com() {
		com.drop();
		DBCursor cursor = coll.find();
		while (cursor.hasNext()) {
			// System.out.println(cursor.next().get("Rdx"));
			// System.out.println(cursor.next().get("Rdy"));
			String rdx = cursor.next().get("Rdx").toString();
			String rdy = cursor.next().get("Rdy").toString();
			float x = Math.round(Float.parseFloat(rdx));
			float y = Math.round(Float.parseFloat(rdy));
			String xy = x + "," + y;
			String json = "{\"Rdxy\": " + "\"" + xy + "\"" + "}";
			System.out.println(json);
			DBObject dbObject = (DBObject) JSON.parse(json);
			com.insert(dbObject);
		}
	}

	public void com1() {
		top3.drop();
		DBCollection com = db.getCollection("com");
		BasicDBObject index = new BasicDBObject("Rdxy", -1);
		com.createIndex(index);
		DBCursor cursor = com.find().sort(index);
		while (cursor.hasNext()) {
			System.out.println(cursor.next());

			if (bidi.containsValue(cursor.curr().get("Rdxy").toString())) {
				int key = bidi.getKey(cursor.curr().get("Rdxy"));
				bidi.put(key + 1, cursor.curr().get("Rdxy").toString());

			} else {
				bidi.put(1, cursor.curr().get("Rdxy").toString());
							}
		}
		
		//System.out.println(bidi.keySet());
		//System.out.println(bidi.keySet());
		Iterator<Integer> hallo = bidi.keySet().iterator();
		while(hallo.hasNext()){
			String key = hallo.next().toString();
			int value = Integer.parseInt(key);
			String hoi = bidi.get(value);
			String[] parts = hoi.split(",");
			String part1 = parts[0]; // 004
			String part2 = parts[1]; // 034556
			x2 = Double.parseDouble(part1);
			y2 = Double.parseDouble(part2);
			convert();
			//System.out.println(key+", "+Latitude+","+Longitude);
			
			String json = "{\"Rdx\": " + "\"" + Latitude + "\"" + ",\"Rdy\": " + "\"" + Longitude + "\""
					+ ", \"Aantal\": " + "" + key + "}";
			System.out.println(json);
			DBObject dbObject = (DBObject) JSON.parse(json);
			top3.insert(dbObject);
			
			
		} 
		
		//System.out.println(bidi.get(48));
		//System.out.println(bidi.size());
		
	}


		
	}


// public void topSpeed() {
//
// DBObject group = new BasicDBObject("$group",
// new BasicDBObject("_id", "$UnitId").append("count", new
// BasicDBObject("$sum",
// 1)));
//
// output = coll.aggregate(group);
//
// for (DBObject result : output.results()) {
// UnitIds.add(result.get("_id").toString());
// String unit = new String();
// unit = result.get("_id").toString();
// DBCollection coll1 = db.getCollection("positions");
//
// BasicDBObject whereQuery = new BasicDBObject();
// whereQuery.put("UnitId", unit);
// DBCursor cursor = coll1.find(whereQuery);
// int i = 0;
// double gereden = 0;
// double l1 = 0;
// double l2 = 0;
// double l3 = 0;
// double l4 = 0;
// int j = 0;
// while (cursor.hasNext()) {
//
// BasicDBObject obj = (BasicDBObject) cursor.next();
// ArrayList<String> x = new ArrayList<String>();
// ArrayList<String> y = new ArrayList<String>();
// x.add(obj.getString("Rdx"));
// y.add(obj.getString("Rdy"));
// String y1 = y.get(0);
// String x1 = x.get(0);
//
// x2 = Double.parseDouble(x1);
// y2 = Double.parseDouble(y1);
// convert();
// i++;
// if (i == 3) {
// i = 1;
// }
// if (i == 1) {
// l1 = Latitude;
// l2 = Longitude;
// } else if (i == 2) {
// l3 = Latitude;
// l4 = Longitude;
// }
// if (distance(l1, l2, l3, l4, "K") < 0.1) {
// if (distance(l1, l2, l3, l4, "K") > 0.03611111) {
// j++;
// topSpeed.add((double) Math.round(distance(l1, l2, l3, l4, "K") * 3600));
// //System.out.println(distance(l1, l2, l3, l4, "K"));
// }
// }
//
// }
// System.out.println("UnitId: " + unit + ", TopSnelheid: " +
// Collections.max(topSpeed) +" Aantal: "+j);
// topSpeed.clear();
// }
//
// }




//kevins group by
//mongo query order by x,y
//maak een HashMap<String><Integer> aantalAggregaat
//for all mongo results
    //afronden x,y van deze row
	//plak in nieuwe string x y aan elkaar (met komma er tussen)
	
	//check of deze nieuwe string bestaat in de hasmap (containskey)
		//haal deze key op uit aantalAggregaat en doe +1
	//else: //zet deze rij ook in aantalAggregaat key,1
//sorteer aantalAggregaat op value
// de bovenste 5 zijn populair






