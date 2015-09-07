package com.rw;

import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.UUID;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import au.com.bytecode.opencsv.CSVReader;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.QueryBuilder;
import com.mongodb.util.JSON;
import com.rw.API.AnalyticsMgr;
import com.rw.API.BusinessMgr;
import com.rw.API.PartnerMgr;
import com.rw.API.RWReqHandle;
import com.rw.API.SecMgr;
import com.rw.API.UserMgr;
import com.rw.Enricher.GoogleGeoCodeImpl;
import com.rw.persistence.JedisMT;
import com.rw.persistence.RWJApplication;
import com.rw.persistence.RWJBusComp;
import com.rw.persistence.RWJBusObj;
import com.rw.persistence.RWObjectMgr;
import com.rw.persistence.RandomDataGenerator;
import com.rw.persistence.mongoMaster;
import com.rw.persistence.mongoRepo;
import com.rw.persistence.mongoStore;
import com.rw.repository.RWAPI;


/**
 *   APIDriver
 *   
 *  java -jar -DPARAM3=STN APIDriver-DROP2.jar -module CFBOutboxMgr -api SendMessages -tenant tenant
 *  java -jar APIDriver-DROP2.jar -module cron
 *  java -jar -classpath ${RWHOME}/Web/target/web/WEB-INF/lib -DPARAM3=STN ${RWHOME}/Web/target/web/WEB-INF/lib/APIDriver-DROP2.jar -module upgrade
 *  java -jar APIDriver-DROP2.jar -module CSVImport -file fileName
 *  java -jar APIDriver-DROP2.jar -module ZipImport -file fileName
 *  java -jar APIDriver-DROP2.jar -module CSVUpdate -file fileName -bc Party 
 *  java -jar APIDriver-DROP2.jar -module addPartner -userid 324234cxcfsf -partnerid erewrw432442
 *  java -jar APIDriver-DROP2.jar -module setVersion -ver 2
 *  java -jar APIDriver-DROP2.jar -module createUser -login someone@somedomain.com -password welcome123 -firstname Some -lastname One -phone "(925)xxx1685" -company "Some Business Development" -profession "some Profession" -photo "photourl"
 *  java -jar APIDriver-DROP2.jar -module addToNetwork -userid 517a032730047ded15eaf2b3 -partyid 517a032730047ded15eaf2b4
 */
public class Main {
    // String S3root = ResourceBundle.getBundle("env", new Locale("en", "US")).getString("S3ROOT");
	static final Logger log = Logger.getLogger("APIDriver");

    public static void main( String[] args ) {
    	
		JSONObject payload = new JSONObject();
		JSONObject retVal = null;

    	Options options = new Options();
    	
    	options.addOption("module", true, "module name");
    	options.addOption("api", true, "api name");
    	options.addOption("file", true, "file Name");
       	options.addOption("userid", true, "userid");
       	options.addOption("partnerid", true, "partnerid");
       	options.addOption("partyid", true, "partyid");
       	options.addOption("ver", true, "ver");
       	options.addOption("login", true, "login");
       	options.addOption("password", true, "password");
       	options.addOption("firstname", true, "firstname");
       	options.addOption("lastname", true, "lastname");
       	options.addOption("phone", true, "phone");
       	options.addOption("company", true, "company");
     	options.addOption("profession", true, "profession");
     	options.addOption("photo", true, "photo");
    	options.addOption("bc", true, "bc");
       	options.addOption("tenant", true, "tenant");
       	options.addOption("connection", true, "connection");
           	     	           	
    	CommandLineParser parser = new BasicParser();
    	
    	try {
			CommandLine cmd = parser.parse( options, args);
    	
			String module = cmd.getOptionValue("module");
			
			System.out.println("Module = " + module);
			
			if(cmd.hasOption("connection")){ 
				System.setProperty("JDBC_CONNECTION_STRING", cmd.getOptionValue("connection"));
			}
			
			if (module.equals("cron")){
				
				
			}

			if (module.equals("dumpbio")){
				
				dumpbiodata();
				return;
			}
			
			if (module.equals("upgrade")){
				mongoStore s = new mongoStore("STN");
				
				BasicDBObject index = new BasicDBObject();
				index.put("GeoWorkLocation", "2d");
				addGeoLocation("Party","Party",s);
				    	// s.getColl("rwParty").ensureIndex(index);
		    	/*
				
		    	s.getColl("rwEvent").ensureIndex(index);
		    	
				addGeoLocation("Event","Event",s);
				
				
				addGeoLocation("Organization","Organization",s);
				
				
				

				
				updateSpeakerStats();
				
				setAllUpperCaseFields("Party","Party");
				
				setupOpenInvitations();
				
				
				
				loadSeedDelta();
				updatePartyTypeDenorm();
				*/
				//setupOpenInvitations();
				// copyGeoToEvent();
			}
			
			if (module.equals("CFBOutboxMgr") ) {
				
				String className = new String ( "com.rw.API." + module );
				Class cls = Class.forName(className);
				RWReqHandle obj = (RWReqHandle) cls.newInstance(); 
				payload.put("act", cmd.getOptionValue("api"));
				
				retVal = obj.handleRequest(payload);

			}
			
			if (module.equals("CSVImport")) {
				String fileName = cmd.getOptionValue("file");
				CSVReader reader = new CSVReader(new FileReader(fileName));
				
    			String [] nextLine = reader.readNext();
     			JSONArray users = new JSONArray();
     		    			
    			while ((nextLine = reader.readNext()) != null) {
        			// nextLine[] is an array of values from the line
        			String Name =  nextLine[0];
        			String business = nextLine[1];
        			String workPhone = nextLine[2];
        			String emailAddress = nextLine[3];
        			String profession = nextLine[4];
        			String photoUrl = nextLine[5];
        			
        			String [] nameParts = Name.split(" ");
        			
        			if (!emailAddress.isEmpty()) {
        				SecMgr sm = new SecMgr();
        				
        				JSONObject data = new JSONObject();
        				JSONObject data2 = new JSONObject();
        			    
        				data.put("act", "create");
        				data.put("login", emailAddress );
        				data.put("password", nameParts[1] + Integer.toString(RandomDataGenerator.random.nextInt(1000)));
       
        				data2.put("login", emailAddress );
        				data2.put("password", data.getString("password"));
        				
        				data.put("passwordHint", "check your email") ;
        				data.put("type", "PARTNER") ;
        				data.put("firstName",nameParts[0]);
        				data.put("lastName",nameParts[1]);
        				data.put("Eula", "No") ;
        				data.put("workPhone", workPhone);
        				data.put("business", business);
        				data.put("profession", profession);
           				data.put("photoUrl", photoUrl);
           			       				
        				createUser(data);
        				
        				//users.put(data2);
 
        			}
        		}
    			

    			
    			/*
    			for ( int i = 0 ; i < users.length(); i++ ) {
    				
    				JSONObject user = users.getJSONObject(i);
    				String user_login = user.getString("login");
    				String user_pw = user.getString("password");
    				
    				for ( int j = 0; j < users.length(); j++) {
    					if ( i != j ) {
    						JSONObject partner = users.getJSONObject(j);
    						String partner_login = partner.getString("login");
     						String partner_pw = partner.getString("password");
     					    						
     						createPartner(user_login, user_pw, partner_login, partner_pw );
    					}
    				}
    			}
    			*/
			}
			

			
			if (module.equals("ZipImport")) {
				String fileName = cmd.getOptionValue("file");
				CSVReader reader = new CSVReader(new FileReader(fileName));
				
    			String [] nextLine = reader.readNext();
     			JSONArray users = new JSONArray();
     		    			
    			while ((nextLine = reader.readNext()) != null) {
        			// nextLine[] is an array of values from the line
        			String zip =  nextLine[0];
        			String state = nextLine[5];
        			String area_codes = nextLine[8];
        			String [] npas = area_codes.split(",");

        			if ( state.equals("CA") && npas.length > 0) {
        				for( int i = 0; i < npas.length; i++) {
        					insertAZipEntry(npas[i],zip);
        				}
        			}
        		}
			}

			if (module.equals("CSVUpdate")) {
				String fileName = cmd.getOptionValue("file");
				String bc = cmd.getOptionValue("bc");
				CSVReader reader = new CSVReader(new FileReader(fileName));
				
    			String [] nextLine = reader.readNext();
     			JSONArray users = new JSONArray();
     		    			
    			while ((nextLine = reader.readNext()) != null) {
        			// nextLine[] is an array of values from the line
        			String emailAddress = nextLine[3];
        			String profession = nextLine[4];
        			String photoUrl = nextLine[5];
        			
        			if (!emailAddress.isEmpty()) {
        				JSONObject data = new JSONObject();
        			    
        				data.put("profession", profession);
           				data.put("photoUrl", photoUrl);
           			       				
           				UpdateAttrs(bc, "emailAddress", emailAddress, data);
        			}
        		}
    			
			}
			
			if (module.equals("addPartner")) {
				String userid = cmd.getOptionValue("userid");
				String partnerid = cmd.getOptionValue("partnerid");
				createPartnerHack(userid, partnerid);
			}

			// -module setVersion -ver 1.0
			if (module.equals("setVersion")) {
				String ver = cmd.getOptionValue("ver");
				setVer(ver);
			}

			// -module createUser -login loginid -password password -firstname  fist -lastname last -phone phone -company company_name
			
			if (module.equals("createUser")) {
   				JSONObject data = new JSONObject();
				data.put("act", "create");
				data.put("login", cmd.getOptionValue("login") );
				data.put("password", cmd.getOptionValue("password"));

				data.put("passwordHint", "check your email") ;
				data.put("type", "PARTNER") ;
				
				data.put("firstName",cmd.getOptionValue("firstname"));
				data.put("lastName",cmd.getOptionValue("lastname"));
				data.put("Eula", "No") ;
				data.put("workPhone", cmd.getOptionValue("phone") );
				data.put("workPhone", cmd.getOptionValue("phone") );
				data.put("business", cmd.getOptionValue("company"));
				data.put("profession", cmd.getOptionValue("profession"));
   				data.put("photoUrl", cmd.getOptionValue("photo"));

   				createUser(data);
			}

			if (module.equals("addToNetwork")) {
				String userid = cmd.getOptionValue("userid");
				String partyid = cmd.getOptionValue("partyid");

				RWJApplication l_app = new RWJApplication();
				RWJBusComp bc = l_app.GetBusObject("Party").getBusComp("Party");
				BasicDBObject query = new BasicDBObject();
				query.put("partytype", "PARTNER");
				
				int nRecs = bc.ExecQuery(query,null);
				
				for ( int i = 0; i < nRecs ; i++) {
					String credid = bc.GetFieldValue("credId").toString();
					String partnerid = bc.GetFieldValue("_id").toString();
					if ( !credid.equals(userid) && !credid.equals("51685b87e4b03b6b2a4c1ad4") ) {
						createPartnerHack(userid, partnerid);
						createPartnerHack(credid, partyid);
					}
					bc.NextRecord();
				}
			}

		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			log.debug("APIDriver Error: ", e);
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			log.debug("APIDriver Error: ", e);
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			log.debug("APIDriver Error: ", e);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			log.debug("APIDriver Error: ", e);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			log.debug("APIDriver Error: ", e);
		}
    }
    
    public static void createUser(JSONObject data) throws JSONException {
		SecMgr sm = new SecMgr();
		try {
			sm.handleRequest(data);
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			log.debug("APIDriver Error: ", e);
		}
    }
    
    public static void createPartner(String login, String upw, String partner, String ppw) throws JSONException {
    	
		SecMgr sm = new SecMgr();
		
		JSONObject data = new JSONObject();
		data.put("act", "login");
		data.put("login", login);
		data.put("password",upw);
		
		try {
			data = sm.handleRequest(data);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			log.debug("APIDriver Error: ", e);
		}
		
		String id = data.get("data").toString();
		data = new JSONObject();

		data.put("act", "login");
		data.put("login", partner);
		data.put("password",ppw);
	
		try {
			data = sm.handleRequest(data);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			log.debug("APIDriver Error: ", e);
		}

		String partnerId = data.get("data").toString();
		data = new JSONObject();

		data.put("act", "read");
		data.put("userid",partnerId);

		UserMgr um = new UserMgr(id);
		
		try {
			data = um.handleRequest(data);
			System.out.println(data);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		BasicDBObject partnerObj = (BasicDBObject) JSON.parse(data.get("data").toString()); 
		ObjectId idObj = (ObjectId) partnerObj.get("_id");
		
		String partnerPartyId = idObj.toString();
		
		data.put("act", "create");
		data.put("partnerId",partnerPartyId);
		
		PartnerMgr pm = new PartnerMgr(id);
		try {
			data = pm.handleRequest(data);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			log.debug("APIDriver Error: ", e);
		}
    }

    public static void createPartnerHack(String userid,  String partnerid) throws JSONException {
    	
		JSONObject data = new JSONObject();
	
		data.put("act", "create");
		data.put("partnerId",partnerid);
		
		PartnerMgr pm = new PartnerMgr(userid);
		try {
			data = pm.handleRequest(data);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			log.debug("APIDriver Error: ", e);
		}
    }

    public static void setVer(String ver) throws Exception {
		RWJApplication l_app = new RWJApplication();
		RWJBusComp bc = l_app.GetBusObject("Admin").getBusComp("Admin");
		BasicDBObject query = new BasicDBObject();
		int nRecs = bc.UpsertQuery(query);
		if ( nRecs == 0 ) bc.NewRecord();
		bc.SetFieldValue("ver", ver);
		bc.SetFieldValue("start",new Date());
		bc.SaveRecord();
	}
    
    public static void UpdateAttrs(String bcName, String primaryKey, String primaryKeyValue, JSONObject data) throws Exception {
		RWJApplication l_app = new RWJApplication();
		RWJBusComp bc = l_app.GetBusObject(bcName).getBusComp(bcName);
		BasicDBObject query = new BasicDBObject();
		
		query.put(primaryKey, primaryKeyValue);
		
		int nRecs = bc.UpsertQuery(query);
		if ( nRecs == 0 ) return;
		bc.SaveRecord(data);
	}
    
    public static void insertAZipEntry(String area_code, String zip) throws Exception {
    	
    	RWJApplication l_app = new RWJApplication();
		RWJBusComp bc = l_app.GetBusObject("Zip").getBusComp("Zip");
		
		bc.NewRecord();
		
		try {
			bc.SetFieldValue("npa", area_code);
			bc.SetFieldValue("zip", zip);
		} catch (java.text.ParseException e) {
			// TODO Auto-generated catch block
			log.debug("APIDriver Error: ", e);
		}
		bc.SaveRecord();
    }
    
    public static void addGeoLocation(String boName, String bcName, mongoStore s) throws Exception{
    	RWObjectMgr context = new RWObjectMgr();
    	RWJApplication app = new RWJApplication(context);
    	context.setExecutionContextItem("tenant", "STN");
    	RWJBusComp bc = app.GetBusObject(boName).getBusComp(bcName);
    	String collName = (bcName.equals("Event"))?"rwEvent":"rwParty";
    	
		
		BasicDBObject query = new BasicDBObject();
		if (bcName.equals("Party")){
			query.put("partytype", "PARTNER");				
		}
		if (bcName.equals("Organization")){
			query.put("partytype", "BUSINESS");
		}
		
		QueryBuilder qb = new QueryBuilder().start().put("GeoWorkLocation.Latitude").exists(false);
		QueryBuilder qb2 = new QueryBuilder().start().put("postalCodeAddress_work").exists(true);
		
		query.putAll(qb.get().toMap());		
		query.putAll(qb2.get().toMap());		
		
		// Updatable by the creator only.
		// query.put("credId", userid.toString());
        
        int nRecs = bc.ExecQuery(query,null);
		
        //System.out.println("nRecs = " + nRecs);
        GoogleGeoCodeImpl geo = new GoogleGeoCodeImpl();
        BasicDBList members = bc.GetCompositeRecordList();
        
        
       
        for (int i = 0;i < nRecs;i++){
			BasicDBObject doc = (BasicDBObject)members.get(i);
			ObjectId dboId = (ObjectId)doc.get("_id");
			String partyId = dboId.toString();
			boolean hasCity = false;
			boolean hasState = false;
			boolean hasZip = false;
			
        	String lField = "GeoWorkLocation";//searchRequestTerm.getString("distanceCompareTo");
        	BasicDBObject compareToAddr = new BasicDBObject();
        	if (doc.containsField("streetAddress1_work")  && !doc.getString("streetAddress1_work").equals("")){compareToAddr.put("streetAddress",doc.getString("streetAddress1_work"));}
        	if (doc.containsField("cityAddress_work") && !doc.getString("cityAddress_work").equals("")){compareToAddr.put("city",doc.getString("cityAddress_work"));hasCity=true;}
        	if (doc.containsField("stateAddress_work") && !doc.getString("stateAddress_work").equals("")){compareToAddr.put("state",doc.getString("stateAddress_work"));hasState=true;}
        	if (doc.containsField("postalCodeAddress_work") && !doc.getString("postalCodeAddress_work").equals("")){compareToAddr.put("postalCode",doc.getString("postalCodeAddress_work"));hasZip=true;}

        	
        	if (hasCity && hasState || hasZip){
	        	BasicDBObject loc = geo.getEnrichmentData(compareToAddr);
	        	if (loc != null){
	        		//bc.setCurrentRecord(i);
		        	//bc.SetFieldValue("GeoWorkLocation",loc);
		        	//bc.SaveRecord(new JSONObject(bc.currentRecord.toString()));
	        		
	        		/*					if (loc.size() > 0){
						Double lngDbl = loc.getDouble("Longitude");
						Double latDbl = loc.getDouble("Latitude");
						bc.SetFieldValue("longitude_work", lngDbl.toString());
						bc.SetFieldValue("latitude_work", latDbl.toString());
					}
					*/
		        	

	        		
		        	BasicDBObject find = new BasicDBObject();
		        	find.put("_id",dboId);

		        	BasicDBObject update = new BasicDBObject();    	
		        	BasicDBObject rwc =  new BasicDBObject();
		        	rwc.put("GeoWorkLocation", loc);
		        	
	        		if (loc.size() > 0){
	        			Double lngDbl = loc.getDouble("Longitude");
						Double latDbl = loc.getDouble("Latitude");
						rwc.put("latitude_work", latDbl);
						rwc.put("longitude_work", lngDbl);
	        		}
	        		update.put("$set",rwc);
		        	s.getColl(collName).updateMulti(find, update);
		        	
	        	}
        	}
			
        }
        
        
    }
    
    private static void loadSeedDelta() throws Exception {
    	
    	
    	SeedDataLoader sl = new SeedDataLoader("STN");
    	
		sl.CleanupSeedData();
		sl.LoadSeedData("rwLov", "/LovSeedData.xml");
		
		sl.LoadSeedData("rwLOVMap", "/LovSeedData.xml");
		sl.LoadLOVMaps("/SeedLOVMaps.xml");
		
		sl.LoadSeedData("rwDQRule", "/dqRules.xml");
		
		sl.LoadSavedSearch("/SavedSearchesSeed.xml","rwSavedSearch","SavedSearches");
		
    }
    
    private static void setAllUpperCaseFields(String bcName, String boName) throws Exception{
    	
    	String upperSuffix = "_upper";
    	RWObjectMgr context = new RWObjectMgr();
    	RWJApplication app = new RWJApplication(context);
    	context.setExecutionContextItem("tenant", "STN");
    	
    	RWJBusObj bo = app.GetBusObject(boName);
		BasicDBObject query = new BasicDBObject();
		query.put("name", bcName);
		mongoStore store = new mongoStore(bo.getApp().getContext().getTenantKey());
		mongoRepo repo = new mongoRepo(bo.getApp().getContext().getTenantKey());
		BasicDBObject metadata = (BasicDBObject) repo.getColl("BusComp").findOne(query);
    	
		BasicDBList columns = (BasicDBList) metadata.get("field");
		String dataClass = metadata.get("dataclass").toString();
		
		for (int i=0; i < columns.size(); i++) {
			BasicDBObject col = (BasicDBObject) columns.get(i);
			String fldname = col.get("fldname").toString();
			if (col.containsField("upperfield")){
				setThisUpperVal(fldname,fldname+upperSuffix,app, bcName, boName, dataClass,store);
				
			}
		}
    }
    
    private static void copyGeoToEvent() throws Exception {
    	RWObjectMgr context = new RWObjectMgr();
    	RWJApplication l_app = new RWJApplication(context);
    	context.setExecutionContextItem("tenant", "STN");
		RWJBusComp bc = l_app.GetBusObject("Event").getBusComp("Event");
		BasicDBObject query = new BasicDBObject();
		int nRecs = bc.UpsertQuery(query);
		int totalUpdates = 0;
		for (int i=0;i < nRecs; i++){
			RWJBusComp org = getOrg((String)bc.GetFieldValue("OrgId"),l_app);
				if( org != null && org.recordSet.size() > 0 ) {
				BasicDBObject GeoWorkLocation = (BasicDBObject) org.GetFieldValue("GeoWorkLocation");
				if( GeoWorkLocation != null) {
				bc.SetFieldValue("GeoWorkLocation", GeoWorkLocation );
				try {
					bc.SaveRecord();
					totalUpdates++;
				}
				catch (com.mongodb.WriteConcernException e) {
						System.out.println("OrgId " + bc.get("OrgId"));
						log.debug("APIDriver Error: ", e);
				}
				}
				}
			bc.NextRecord();
		}
		
		System.out.println("done copying geo events, Total :" + totalUpdates);
		
    }
    
    private static RWJBusComp getOrg(String OrgId,RWJApplication app) throws Exception{
		RWJBusComp bc = app.GetBusObject("Organization").getBusComp("Organization");
		BasicDBObject query = new BasicDBObject();
		query.put("_id", new ObjectId(OrgId));
		bc.ExecQuery(query);
		return bc;
	}
	
    
    private static void setThisUpperVal(String fieldName, String setFieldName,RWJApplication app, String bcName, String boName, String dataClass, mongoStore s) throws Exception{
    	BasicDBObject query = new BasicDBObject();
    	//find.put("_id",dboId);
    	RWJBusComp bc = app.GetBusObject(boName).getBusComp(bcName);
    	
    	
    	BasicDBObject all = new BasicDBObject();
		BasicDBObject neInner = new BasicDBObject();
		neInner.put("$exists", true);
		BasicDBObject neOuter = new BasicDBObject();
		neOuter.put(fieldName, neInner);
		BasicDBObject emptyString = new BasicDBObject();
		emptyString.put("$ne", "");
		BasicDBObject emptyStringOuter = new BasicDBObject();
		emptyStringOuter.put(fieldName,emptyString);
		BasicDBObject nul = new BasicDBObject();
		nul.put(fieldName,null);
		BasicDBObject nullOuter = new BasicDBObject();
		nullOuter.put("$ne",nul);
		BasicDBList allAnd = new BasicDBList();
		allAnd.add(neOuter);
		allAnd.add(emptyStringOuter);
		//allAnd.add(nullOuter);
		query.put("$and", allAnd);
    	
		int nRecs = bc.ExecQuery(query,null);
		BasicDBList records = bc.recordSet;
		for (int i = 0;i < nRecs;i++){
			BasicDBObject doc = (BasicDBObject)records.get(i);
			ObjectId dboId = (ObjectId)doc.get("_id");
			String sId = dboId.toString();
			String fieldVal = doc.getString(fieldName).toUpperCase();
			
			BasicDBObject find = new BasicDBObject();
        	find.put("_id",dboId);

        	BasicDBObject update = new BasicDBObject();    	
        	BasicDBObject rwc =  new BasicDBObject();
        	rwc.put(setFieldName, fieldVal);
        	update.put("$set",rwc);
        	
        	s.getColl(dataClass).updateMulti(find, update);
		}
    }
    
    public static void setupOpenInvitations() throws Exception {
    	
    	RWObjectMgr context = new RWObjectMgr();
    	RWJApplication app = new RWJApplication(context);
    	context.setExecutionContextItem("tenant", "STN");
    	
    	RWJBusComp bc = app.GetBusObject("Party").getBusComp("Party");
    	BasicDBObject query = new BasicDBObject();
    	query.put("partytype", "PARTNER");
    	int nRecs = bc.UpsertQuery(query);  	
    	UserMgr um = new UserMgr(context);
    	
    	for (int i=0; i < nRecs; i++ ) {
    		um.SetupOpenInvitaton(bc);
    		bc.NextRecord();
    	}
   	
    }
    
	

	
	private static void updateSpeakerStats() throws Exception{
		
		RWObjectMgr context = new RWObjectMgr();
    	RWJApplication app = new RWJApplication(context);
    	context.setExecutionContextItem("tenant", "STN");
    	
    	AnalyticsMgr a = new AnalyticsMgr(context);	
		
    	
    	RWJBusComp bc = app.GetBusObject("Party").getBusComp("Party");
    	BasicDBObject query = new BasicDBObject();
    	//query.put("partytype", "PARTNER");
    	query.put("isSpeaker", "true");
    	int nRecs = bc.UpsertQuery(query);
    	
    	
    	for (int i=0; i < nRecs; i++ ) {
			String speakerId = bc.get("_id").toString();
    		
    		JSONObject data_in = new JSONObject();		
    		data_in.put("bo", "Party");
    		data_in.put("partyId", speakerId);
    		data_in.put("userid", speakerId);
    		data_in.put("lastlogin_date", "");
    		data_in.put("OrgId", "foo");
    		data_in.put("bc", "Party");
    		data_in.put("FK", speakerId);
    		data_in.put("summarizer", "totalSpeakerEngagements"); //"totalSpeakerEngagements"
    		data_in.put("chartName", "SpeakerEngagement");//"SpeakerEngagement"
    		data_in.put("Speaker1_Id", speakerId);
    		a.summarize(data_in);
    		bc.NextRecord();
    		

    		
    	}
	
	}
	

	private static void dumpbiodata() throws Exception{
		
		RWObjectMgr context = new RWObjectMgr();
    	RWJApplication app = new RWJApplication(context);
    	context.setExecutionContextItem("tenant", "STN");
    	
     	RWJBusComp bc = app.GetBusObject("Party").getBusComp("Party");
    	BasicDBObject query = new BasicDBObject();
    	query.put("partytype", "PARTNER");
    	int nRecs = bc.ExecQuery(query);
    	
    	
    	for (int i=0; i < nRecs; i++ ) {
			String Id = bc.get("_id").toString();
			Object bio = bc.get("bio");
			if ( bio != null ) {
				System.out.println(Id);
				System.out.println(bio.toString());
				
				BufferedWriter writer = null;
				try
				{
				    writer = new BufferedWriter( new FileWriter( "/Users/sudhakarkaki/pet/eventscout/tests/" + Id + ".profile"));
				    Object title = bc.get("jobTitle");
				    if (title != null) {
					    writer.write( title.toString() + "\n");				    	
				    }
				    else 
					    writer.write("ES:NOJOBTITLE\n");

				    Object fn = bc.get("firstName");
				    if (fn != null) {
					    writer.write( fn.toString() + "\n");				    	
				    }
				    else 
					    writer.write("ES:NOFIRSTNAME\n");

				    Object ln = bc.get("lastName");
				    if (fn != null) {
					    writer.write( ln.toString() + "\n");				    	
				    }
				    else 
					    writer.write("ES:NOLASTNAME\n");

				    writer.write( bio.toString());

				}
				catch ( IOException e)
				{
				}
				finally
				{
				    try
				    {
				        if ( writer != null)
				        writer.close( );
				    }
				    catch ( IOException e)
				    {
				    }
				}
				
				
				
				
			}
			bc.NextRecord();
    		
    	}
	
	}

	private static void updatePartyTypeDenorm() throws Exception{
		RWObjectMgr context = new RWObjectMgr();
    	RWJApplication app = new RWJApplication(context);
    	context.setExecutionContextItem("tenant", "STN");
    	
    	RWJBusComp bc = app.GetBusObject("Partner").getBusComp("Partner");
    	BasicDBObject query = new BasicDBObject();
    	//query.put("partytype", "PARTNER");
    	
    	int nRecs = bc.UpsertQuery(query); 
    	RWJBusComp ptybc = app.GetBusObject("Party").getBusComp("Party");
    	
    	
    	
    	
    	for (int i=0; i < nRecs; i++ ) {
    		if (i % 1000 == 0){System.out.println(i + " records");};
    		String partnerId = bc.GetFieldValue("partnerId").toString();
    		query = new BasicDBObject();
    		query.put("_id", new ObjectId(partnerId));
    		int r = ptybc.ExecQuery(query);
    		if (r > 0){
        		String partytype = (String)ptybc.GetFieldValue("partytype");
        		bc.SetFieldValue("partytype_denorm", partytype);
        		bc.SaveRecord();
    		}
    		bc.NextRecord();

    	}
		
	}
    
}
