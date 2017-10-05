import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import javax.naming.spi.DirStateFactory.Result;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.io.FileInputStream;

/**
 * Runs queries against a back-end database
 */
public class Query {

	private String configFilename;
	private Properties configProps = new Properties();

	private String jSQLDriver;
	private String jSQLUrl;
	private String jSQLUser;
	private String jSQLPassword;
	private int max;
	private boolean loggedIn = false;
	// DB Connection
	private Connection conn;

	// stores local searches
	private ArrayList<Integer> localSearch;
	// records where one-hop-flights end and two-hop-flights begin
	private int directDivider;
	// stores local reservations
	private ArrayList<Integer> localReservation;

	// Logged In User
	private String username;
        private int cid; // Unique customer ID

	// Canned queries

       // search (one hop) -- This query ignores the month and year entirely. You can change it to fix the month and year
       // to July 2015 or you can add month and year as extra, optional, arguments
	private static final String SEARCH_ONE_HOP_SQL =
			"SELECT TOP (?) fid,year,month_id,day_of_month,carrier_id,flight_num,origin_city,actual_time "
					+ "FROM Flights "
					+ "WHERE origin_city = ? AND dest_city = ? AND year = 2015 AND month_id = 7 AND day_of_month = ? AND actual_time IS NOT NULL "
					+ "ORDER BY actual_time ASC";
	private PreparedStatement searchOneHopStatement;
	
	// This query searches for two hop flights in July 2015, given day of month, order by total time
	private static final String SEARCH_TWO_HOP_SQL = 	
			"SELECT TOP (?) f1.fid as f1_fid, f1.year,f1.month_id,f1.day_of_month,f1.flight_num as f1_flightNum,f1.origin_city as f1_origin_city, f1.dest_city as f1_dest_city, f1.carrier_id as f1_carrierID, f1.actual_time as f1_actualTime, "
			+ "f2.fid as f2_fid, f2.flight_num as f2_flight_num, f2.carrier_id as f2_carrier_id, f2.origin_city as f2_origin_city, f2.dest_city as f2_dest_city, f2.actual_time as f2_actualTime, f1.actual_time + f2.actual_time as totalTime "
			+ "FROM Flights f1, Flights f2 "
			+ "WHERE f1.origin_city = ? AND f2.dest_city = ? AND f1.day_of_month = ? "
			+ "AND f1.dest_city = f2.origin_city AND "
			+ "f1.month_id = 7 AND f1.month_id = f2.month_id AND f1.day_of_month = f2.day_of_month "
			+ "AND f1.year = 2015 AND f2.year = 2015 "
			+ "AND f1.actual_time IS NOT NULL AND f2.actual_time IS NOT NULL "
			+ "ORDER BY totalTime ASC";
	private PreparedStatement searchTwoHopStatement;
	
	// This query finds the password for given user.
	private static final String LOGIN_SQL = 
			"SELECT password "
			+ "FROM customer "
			+ "WHERE username = ?";
	private PreparedStatement loginStatement;

	// This query returns all reservations for given user
	private static final String RESERVATION_SQL = 
			"SELECT * "
			+ "FROM reservations "
			+ "WHERE username = ?" ;
	private PreparedStatement reservationStatement;
	
	// finds all reservations for given user and day of month
	private static final String RESERVATION_FIND_SQL = 
			"SELECT * "
			+ "FROM reservations "
			+ "WHERE username = ? "
			+ "AND day_of_month = ? ";
	PreparedStatement reservationFindStatement;
	
	// C2: finds the number of reservations for given user and day of month
	private static final String RESERVATION_SIZE_SQL = 
			"SELECT count(*) as size "
			+ "FROM reservations "
			+ "WHERE username = ? "
			+ "AND day_of_month = ? ";
	PreparedStatement reservationSizeStatement;
	
	// finds info on given fid
	private static final String FLIGHT_SEARCH_SQL = 
			"SELECT * "
			+ "FROM Flights "
			+ "WHERE fid = ?";
	private PreparedStatement flightSearchStatement;

	// insert a set of tuples which completes a booking transaction
	private static final String BOOK_SQL = 
			"INSERT INTO reservations values(?, ?, ?,?)";
	private PreparedStatement bookStatement;
	
	// finds the maximum index in reservations table
	private static final String MAX_SQL = 
			"SELECT MAX(rid) as Max "
			+ "FROM reservations ";
	private PreparedStatement MaxStatement;
	
	// cancels a given reservation
	private static final String CANCEL_SQL = 
			"DELETE FROM reservations "
			+ "WHERE rid = ? ";
	private PreparedStatement CancelStatement;
	
	// updates the capacity for given flight
	private static final String ADD_CAPACITY_SQL =
			"UPDATE flights "
			+ "SET capacity += 1 "
			+ "WHERE fid = ? ";
	private PreparedStatement AddCapacityStatement;
	
	// gets the capacity for given flight
	private static final String GET_CAPACITY_SQL = 
			"SELECT capacity "
			+ "FROM flights "
			+ "WHERE fid = ?";
	private PreparedStatement GetCapacityStatement;

	
	// transactions
	private static final String BEGIN_TRANSACTION_SQL =  
			"SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; BEGIN TRANSACTION;"; 
	private PreparedStatement beginTransactionStatement;

	private static final String COMMIT_SQL = "COMMIT TRANSACTION";
	private PreparedStatement commitTransactionStatement;

	private static final String ROLLBACK_SQL = "ROLLBACK TRANSACTION";
	private PreparedStatement rollbackTransactionStatement;


	public Query(String configFilename) {
		this.configFilename = configFilename;
	}

	/**********************************************************/
	/* Connection code to SQL Azure.  */
	public void openConnection() throws Exception {
		configProps.load(new FileInputStream(configFilename));

		jSQLDriver   = configProps.getProperty("flightservice.jdbc_driver");
		jSQLUrl	   = configProps.getProperty("flightservice.url");
		jSQLUser	   = configProps.getProperty("flightservice.sqlazure_username");
		jSQLPassword = configProps.getProperty("flightservice.sqlazure_password");

		/* load jdbc drivers */
		Class.forName(jSQLDriver).newInstance();

		/* open connections to the flights database */
		conn = DriverManager.getConnection(jSQLUrl, // database
				jSQLUser, // user
				jSQLPassword); // password

		conn.setAutoCommit(false); //by default automatically commit after each statement 
		conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);

		/* You will also want to appropriately set the 
                   transaction's isolation level through:  
		   conn.setTransactionIsolation(...) */

	}

	public void closeConnection() throws Exception {
		conn.close();
	}

	/**********************************************************/
	/* prepare all the SQL statements in this method.
      "preparing" a statement is almost like compiling it.  Note
       that the parameters (with ?) are still not filled in */

	public void prepareStatements() throws Exception {
		localSearch = new ArrayList<Integer>();
		localReservation = new ArrayList<Integer>();
		
 		beginTransactionStatement = conn.prepareStatement(BEGIN_TRANSACTION_SQL);
		commitTransactionStatement = conn.prepareStatement(COMMIT_SQL);
		rollbackTransactionStatement = conn.prepareStatement(ROLLBACK_SQL);

		searchOneHopStatement = conn.prepareStatement(SEARCH_ONE_HOP_SQL);
		searchTwoHopStatement = conn.prepareStatement(SEARCH_TWO_HOP_SQL);
		loginStatement = conn.prepareStatement(LOGIN_SQL);
		reservationStatement = conn.prepareStatement(RESERVATION_SQL);
		reservationFindStatement = conn.prepareStatement(RESERVATION_FIND_SQL);
		reservationSizeStatement = conn.prepareStatement(RESERVATION_SIZE_SQL);
		flightSearchStatement = conn.prepareStatement(FLIGHT_SEARCH_SQL);
		bookStatement = conn.prepareStatement(BOOK_SQL);
		MaxStatement = conn.prepareStatement(MAX_SQL);
		CancelStatement = conn.prepareStatement(CANCEL_SQL);
		AddCapacityStatement = conn.prepareStatement(ADD_CAPACITY_SQL);
		GetCapacityStatement = conn.prepareStatement(GET_CAPACITY_SQL);

	}
	
	// user logs in with a legal combination of username and password
	public void transaction_login(String username, String password) throws Exception {
		loginStatement.clearParameters();
		loginStatement.setString(1, username);
		ResultSet loginResults = loginStatement.executeQuery();
		if (loginResults.next()) {
			if (password.equals(loginResults.getString("password"))) {
				loggedIn = true;
				this.username = username;
				System.out.println("You have successfully logged in as: " + username);
			}else {
				System.out.println("Please try a different username or password.");
			}
		}else {
			System.out.println("Please try a different username or password.");
		}
	}

	/**
	 * Searches for flights from the given origin city to the given destination
	 * city, on the given day of the month. If "directFlight" is true, it only
	 * searches for direct flights, otherwise is searches for direct flights
	 * and flights with two "hops". Only searches for up to the number of
	 * itineraries given.
	 * Prints the results found by the search.
	 */
	public void transaction_search_safe(String originCity, String destinationCity, boolean directFlight, int dayOfMonth, int numberOfItineraries) throws Exception {
		// one hop itineraries

		localSearch.clear();
		searchOneHopStatement.clearParameters();
		searchOneHopStatement.setInt(1, numberOfItineraries);
		searchOneHopStatement.setString(2, originCity);
		searchOneHopStatement.setString(3, destinationCity);
		searchOneHopStatement.setInt(4, dayOfMonth);
		ResultSet oneHopResults = searchOneHopStatement.executeQuery();
    	int count = 0;
        if (!oneHopResults.next()) {
        	System.out.println("Sorry, no direct flight matches your search criteria.");
        }else {
        	System.out.println("Here are the direct flights:");
        	while (oneHopResults.next()) {
    			int result_fid = oneHopResults.getInt("fid");
                int result_year = oneHopResults.getInt("year");
                int result_monthId = oneHopResults.getInt("month_id");
                int result_dayOfMonth = oneHopResults.getInt("day_of_month");
                String result_carrierId = oneHopResults.getString("carrier_id");
                String result_flightNum = oneHopResults.getString("flight_num");
                String result_originCity = oneHopResults.getString("origin_city");
                int result_time = oneHopResults.getInt("actual_time");
            	count++;
                System.out.println("Flight no." + count + ": " +  result_year + "," + result_monthId + "," + result_dayOfMonth + "," + result_carrierId + "," + result_flightNum + "," + result_originCity + "," + result_time + "," + result_fid);
            	localSearch.add(result_fid);
            	if (count >= numberOfItineraries) {
            		break;
            	}
        	}
		}
	        // this records where one-hop ends and two-hop begins
        directDivider = count;
		oneHopResults.close();
		if (!directFlight && numberOfItineraries - count > 0) {
			searchTwoHopStatement.clearParameters();
			searchTwoHopStatement.setInt(1, numberOfItineraries);
			searchTwoHopStatement.setString(2, originCity);
			searchTwoHopStatement.setString(3, destinationCity);
			searchTwoHopStatement.setInt(4, dayOfMonth);
			ResultSet twoHopResults = searchTwoHopStatement.executeQuery();
	        if (!twoHopResults.next()) {
	        	System.out.println("Sorry, no one-hop flight matches your search criteria.");
	        }else {
	        	System.out.println("Here are the hopping flights:");
	
	        	while (twoHopResults.next()) {
                    int result_year = twoHopResults.getInt("year");
                	int result_monthId = twoHopResults.getInt("month_id");
                    int result_dayOfMonth = twoHopResults.getInt("day_of_month");
                    int f1_fid = twoHopResults.getInt("f1_fid");
                    int f2_fid = twoHopResults.getInt("f2_fid");
	                    
                    String f1_result_carrierId = twoHopResults.getString("f1_carrierId");
                    String f1_result_flightNum = twoHopResults.getString("f1_flightNum");
                    String f2_result_carrierId = twoHopResults.getString("f2_carrier_id");
                    String f2_result_flightNum = twoHopResults.getString("f2_flight_num");
                    String f1_result_originCity = twoHopResults.getString("f1_origin_city");
                    String f2_result_originCity = twoHopResults.getString("f2_origin_city");
                    String f1_result_destCity = twoHopResults.getString("f1_dest_city");
                    String f2_result_destCity = twoHopResults.getString("f2_dest_city");
                    int f1_actualTime = twoHopResults.getInt("f1_actualTime");
                    int f2_actualTime = twoHopResults.getInt("f2_actualTime");
//	                int total_time = twoHopResults.getInt("totalTime");
                	count++;
                    System.out.println("Flight no." + count + ": " + result_year + "," + result_monthId + "," + result_dayOfMonth + "," + f1_result_carrierId + "," + f1_result_flightNum + ","
                    + f1_result_originCity + ","+ f1_result_destCity + "," + f1_actualTime + "," + f1_fid);
                	// save the results to localSearch
                    localSearch.add(f1_fid);
                    System.out.println("           + " +  result_year + "," + result_monthId + "," + result_dayOfMonth + "," + f2_result_carrierId + "," + f2_result_flightNum + "," 
                    + f2_result_originCity + ","+ f2_result_destCity + "," + f2_actualTime + "," + f2_fid);
                    localSearch.add(f2_fid);

                    if (count >= numberOfItineraries) {
                		break;
                	}
	        	}
			}
			twoHopResults.close();
        }
	}
	
	public void transaction_search_unsafe(String originCity, String destinationCity, boolean directFlight,int dayOfMonth, int numberOfItineraries) throws Exception {

            // one hop itineraries
            String unsafeSearchSQL =
                "SELECT TOP (" + numberOfItineraries +  ") year,month_id,day_of_month,carrier_id,flight_num,origin_city,actual_time "
                + "FROM Flights "
                + "WHERE origin_city = \'" + originCity + "\' AND dest_city = \'" + destinationCity + "\' AND year = 2015 " + " AND month_id = 7 " +
                "AND day_of_month =  " + dayOfMonth + " " + "ORDER BY actual_time ASC";

            System.out.println("Submitting query: " + unsafeSearchSQL);
            Statement searchStatement = conn.createStatement();
            ResultSet oneHopResults = searchStatement.executeQuery(unsafeSearchSQL);
            if (!oneHopResults.next()) {
            	System.out.println("Sorry, no direct flight matches your search criteria.");
            }
            int count = 0;
            while (oneHopResults.next()) {
                int result_year = oneHopResults.getInt("year");
                int result_monthId = oneHopResults.getInt("month_id");
                int result_dayOfMonth = oneHopResults.getInt("day_of_month");
                String result_carrierId = oneHopResults.getString("carrier_id");
                String result_flightNum = oneHopResults.getString("flight_num");
                String result_originCity = oneHopResults.getString("origin_city");
                int result_time = oneHopResults.getInt("actual_time");
                System.out.println("Flight: " + result_year + "," + result_monthId + "," + result_dayOfMonth + "," + result_carrierId + "," + result_flightNum + "," + result_originCity + "," + result_time);
            	count++;
            	if (count >= numberOfItineraries) {
            		break;
            	}
            }
            oneHopResults.close();
    
            
        }

	// This handels the booking of tickets
	public void transaction_book(int itineraryId) throws Exception {
		if (loggedIn) {
			if (localSearch.isEmpty()) {
				System.out.println("Please make a search before you make a booking.");
			}else if (itineraryId >= localSearch.size() || itineraryId < 0) {
				System.out.println("Please enter a valid itineraryID.");
			}else {
				// get the maximum index + 1 to be the next rid
				ResultSet MaxResults = MaxStatement.executeQuery();
				while (MaxResults.next()) {
					max = MaxResults.getInt("Max");
				}
				
				// if the requested itinerary is a direct flight
				if(itineraryId < directDivider) {
					book(localSearch.get(itineraryId - 1));
				}else {
					
					// if its a two hop flight, book the first one then the second one
					book(localSearch.get( 2* itineraryId - directDivider - 2));
					MaxResults = MaxStatement.executeQuery();
					while (MaxResults.next()) {
						max = MaxResults.getInt("Max");
					}
					book(localSearch.get( 2* itineraryId - directDivider - 1));
				}
			}
		}else {
			System.out.println("You need to log in before you can book a flight.");
		}
	}

	// private helper method that does the actual booking given fid
	private void book(int fid) throws Exception {
		try {
			beginTransaction();
			GetCapacityStatement.setInt(1, fid);
			ResultSet capacityResult = GetCapacityStatement.executeQuery();
			int capacity = 0;
			if (capacityResult.next()) {
				capacity = capacityResult.getInt("capacity");
			}
			//get the capacity for current fid
			if (capacity < 3) {
				flightSearchStatement.setInt(1, fid);
				ResultSet flightSearchResults= flightSearchStatement.executeQuery();
				int result_dayOfMonth = 0;
				String result_origin_city = "";
				while (flightSearchResults.next()) {
		            result_dayOfMonth = flightSearchResults.getInt("day_of_month");
		            result_origin_city = flightSearchResults.getString("origin_city");
				}				
				reservationFindStatement.setString(1, username);
				reservationFindStatement.setInt(2, result_dayOfMonth);
				reservationSizeStatement.setString(1, username);
				reservationSizeStatement.setInt(2, result_dayOfMonth);
				ResultSet reservationFindResults = reservationFindStatement.executeQuery();
				ResultSet reservationSizeResults = reservationSizeStatement.executeQuery();
				int size = 0;
				if (reservationSizeResults.next()) {
					// finds the number of reservations for the user given the day of month.
					size = reservationSizeResults.getInt("size");
				}
				
				// if its less than 2
				if (size < 2) {
					
					// if it's 1
					if (reservationFindResults.next()) {
						int fid2 = reservationFindResults.getInt("fid");
						flightSearchStatement.setInt(1, fid2);
						ResultSet flightSearchResults2= flightSearchStatement.executeQuery();
						String result_dest_city = "";
						if (flightSearchResults2.next()) {
				            result_dest_city = flightSearchResults2.getString("dest_city");
						}		
						// check if they are two-hop flights by checking if the dest_city and origin_city are the same
						if (result_dest_city.equals(result_origin_city)) {
							bookStatement.setInt(1, max + 1);
							bookStatement.setString(2, username);
							bookStatement.setInt(3, fid);
							bookStatement.setInt(4, result_dayOfMonth);
							bookStatement.execute();
							AddCapacityStatement.setInt(1, fid);
							AddCapacityStatement.execute();
							commitTransaction();
						}else {
							System.out.println("Sorry, but you can only book one itinerary per day.");
							rollbackTransaction();
						}
						
					// if it's 0, directly book the flight
					}else{
						bookStatement.setInt(1, max + 1);
						bookStatement.setString(2, username);
						bookStatement.setInt(3, fid);
						bookStatement.setInt(4, result_dayOfMonth);
						bookStatement.execute();
						AddCapacityStatement.setInt(1, fid);
						AddCapacityStatement.execute();
						commitTransaction();
					}
				}else {
					System.out.println("Sorry, but you can only book one itinerary per day.");
					rollbackTransaction();
				}
			}else {
				System.out.println("The maximum capacity of this flight has been reached.");
				rollbackTransaction();
			}
		} catch (SQLException e) {
			try {
				rollbackTransaction();
			} catch (SQLException se) {
			}
		}
	}
	
	// finds all reservations for given username
	public void transaction_reservations() throws Exception {

		try {
			beginTransaction();
			if (loggedIn) {
				localReservation.clear();
				reservationStatement.setString(1, username);
				ResultSet reservationResults = reservationStatement.executeQuery();
				int count = 1;
				while (reservationResults.next()) {
					int result_rid = reservationResults.getInt("rid");
					int result_fid = reservationResults.getInt("fid");
					System.out.print("Reservation no." + count + " for user " + username + ": ");
					count++;
					
					// retrieve all search results and store them in localSearch
					localReservation.add(result_rid);
					flightSearchStatement.setInt(1, result_fid);
					ResultSet flightSearchResults= flightSearchStatement.executeQuery();
					while (flightSearchResults.next()) {
			            int result_dayOfMonth = flightSearchResults.getInt("day_of_month");
			            String result_carrierId = flightSearchResults.getString("carrier_id");
			            String result_flightNum = flightSearchResults.getString("flight_num");
			            String result_originCity = flightSearchResults.getString("origin_city");
			            String result_destCity = flightSearchResults.getString("dest_city");
			            int result_time = flightSearchResults.getInt("actual_time");
			            System.out.println(result_dayOfMonth + "," + result_carrierId + "," + result_flightNum + "," + result_originCity + "," + result_destCity + " "+ result_time);
					}
				}
				commitTransaction();
			}else {
				System.out.println("Sorry, you must log in before you can see your reservations.");
				rollbackTransaction();
			}
		} catch (SQLException e) {
			try {
				rollbackTransaction();
			} catch (SQLException se) {
			}
		}

	}

	public void transaction_cancel(int reservationId) throws Exception {
//		System.out.println("t:");
//		System.in.read();
		try {
			beginTransaction();
			if (loggedIn) {	
				ResultSet MaxResults = MaxStatement.executeQuery();
				
				// get the current maximum rid
				if (MaxResults.next()) {
					max = MaxResults.getInt("Max");
				}
				
				// user must make a search first
				if (localReservation.isEmpty()){
					System.out.println("You need to display all your reservations first.");
					rollbackTransaction();
				}else if (reservationId < 1 || reservationId > max || reservationId > localReservation.size()) {
					System.out.println("Please enter a valid reservationID.");
					rollbackTransaction();
				}else {
					CancelStatement.setInt(1, localReservation.get(reservationId - 1));
					CancelStatement.execute();
//					System.out.println("ttt:");
//					System.in.read();
					commitTransaction();
				}
			}else {
				System.out.println("You must log in to cancel a reservation.");
				rollbackTransaction();
			}
		} catch (SQLException e) {
			try {
				rollbackTransaction();
			} catch (SQLException se) {
			}
		}
	}

    
   public void beginTransaction() throws Exception {
        conn.setAutoCommit(false);
        beginTransactionStatement.executeUpdate();  
    }

    public void commitTransaction() throws Exception {
        commitTransactionStatement.executeUpdate(); 
        conn.setAutoCommit(true);
    }
    public void rollbackTransaction() throws Exception {
        rollbackTransactionStatement.executeUpdate();
        conn.setAutoCommit(true);
        } 

}
