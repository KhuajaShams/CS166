/*
 * Template JAVA User Interface
 * =============================
 *
 * Database Management Systems
 * Department of Computer Science &amp; Engineering
 * University of California - Riverside
 *
 * Target DBMS: 'Postgres'
 *
 */


import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;
import java.util.ArrayList;

/**
 * This class defines a simple embedded SQL utility class that is designed to
 * work with PostgreSQL JDBC drivers.
 *
 */

public class DBproject{
	//reference to physical database connection
	private Connection _connection = null;
	static BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
	
	public DBproject(String dbname, String dbport, String user, String passwd) throws SQLException {
		System.out.print("Connecting to database...");
		try{
			// constructs the connection URL
			String url = "jdbc:postgresql://localhost:" + dbport + "/" + dbname;
			System.out.println ("Connection URL: " + url + "\n");
			
			// obtain a physical connection
	        this._connection = DriverManager.getConnection(url, user, passwd);
	        System.out.println("Done");
		}catch(Exception e){
			System.err.println("Error - Unable to Connect to Database: " + e.getMessage());
	        System.out.println("Make sure you started postgres on this machine");
	        System.exit(-1);
		}
	}
	
	/**
	 * Method to execute an update SQL statement.  Update SQL instructions
	 * includes CREATE, INSERT, UPDATE, DELETE, and DROP.
	 * 
	 * @param sql the input SQL string
	 * @throws java.sql.SQLException when update failed
	 * */
	public void executeUpdate (String sql) throws SQLException { 
		// creates a statement object
		Statement stmt = this._connection.createStatement ();

		// issues the update instruction
		stmt.executeUpdate (sql);

		// close the instruction
	    stmt.close ();
	}//end executeUpdate

	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and outputs the results to
	 * standard out.
	 * 
	 * @param query the input query string
	 * @return the number of rows returned
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public int executeQueryAndPrintResult (String query) throws SQLException {
		//creates a statement object
		Statement stmt = this._connection.createStatement ();

		//issues the query instruction
		ResultSet rs = stmt.executeQuery (query);

		/*
		 *  obtains the metadata object for the returned result set.  The metadata
		 *  contains row and column info.
		 */
		ResultSetMetaData rsmd = rs.getMetaData ();
		int numCol = rsmd.getColumnCount ();
		int rowCount = 0;
		
		//iterates through the result set and output them to standard out.
		boolean outputHeader = true;
		while (rs.next()){
			if(outputHeader){
				for(int i = 1; i <= numCol; i++){
					System.out.print(rsmd.getColumnName(i) + "\t");
			    }
			    System.out.println();
			    outputHeader = false;
			}
			for (int i=1; i<=numCol; ++i)
				System.out.print (rs.getString (i) + "\t");
			System.out.println ();
			++rowCount;
		}//end while
		stmt.close ();
		return rowCount;
	}
	
	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and returns the results as
	 * a list of records. Each record in turn is a list of attribute values
	 * 
	 * @param query the input query string
	 * @return the query result as a list of records
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public List<List<String>> executeQueryAndReturnResult (String query) throws SQLException { 
		//creates a statement object 
		Statement stmt = this._connection.createStatement (); 
		
		//issues the query instruction 
		ResultSet rs = stmt.executeQuery (query); 
	 
		/*
		 * obtains the metadata object for the returned result set.  The metadata 
		 * contains row and column info. 
		*/ 
		ResultSetMetaData rsmd = rs.getMetaData (); 
		int numCol = rsmd.getColumnCount (); 
		int rowCount = 0; 
	 
		//iterates through the result set and saves the data returned by the query. 
		boolean outputHeader = false;
		List<List<String>> result  = new ArrayList<List<String>>(); 
		while (rs.next()){
			List<String> record = new ArrayList<String>(); 
			for (int i=1; i<=numCol; ++i) 
				record.add(rs.getString (i)); 
			result.add(record); 
		}//end while 
		stmt.close (); 
		return result; 
	}//end executeQueryAndReturnResult
	
	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and returns the number of results
	 * 
	 * @param query the input query string
	 * @return the number of rows returned
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public int executeQuery (String query) throws SQLException {
		//creates a statement object
		Statement stmt = this._connection.createStatement ();

		//issues the query instruction
		ResultSet rs = stmt.executeQuery (query);

		int rowCount = 0;

		//iterates through the result set and count nuber of results.
		if(rs.next()){
			rowCount++;
		}//end while
		stmt.close ();
		return rowCount;
	}
	
	/**
	 * Method to fetch the last value from sequence. This
	 * method issues the query to the DBMS and returns the current 
	 * value of sequence used for autogenerated keys
	 * 
	 * @param sequence name of the DB sequence
	 * @return current value of a sequence
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	
	public int getCurrSeqVal(String sequence) throws SQLException {
		Statement stmt = this._connection.createStatement ();
		
		ResultSet rs = stmt.executeQuery (String.format("Select currval('%s')", sequence));
		if (rs.next()) return rs.getInt(1);
		return -1;
	}

	/**
	 * Method to close the physical connection if it is open.
	 */
	public void cleanup(){
		try{
			if (this._connection != null){
				this._connection.close ();
			}//end if
		}catch (SQLException e){
	         // ignored.
		}//end try
	}//end cleanup

	/**
	 * The main execution method
	 * 
	 * @param args the command line arguments this inclues the <mysql|pgsql> <login file>
	 */
	public static void main (String[] args) {
		if (args.length != 3) {
			System.err.println (
				"Usage: " + "java [-classpath <classpath>] " + DBproject.class.getName () +
		            " <dbname> <port> <user>");
			return;
		}//end if
		
		DBproject esql = null;
		
		try{
			System.out.println("(1)");
			
			try {
				Class.forName("org.postgresql.Driver");
			}catch(Exception e){

				System.out.println("Where is your PostgreSQL JDBC Driver? " + "Include in your library path!");
				e.printStackTrace();
				return;
			}
			
			System.out.println("(2)");
			String dbname = args[0];
			String dbport = args[1];
			String user = args[2];
			
			esql = new DBproject (dbname, dbport, user, "");
			
			boolean keepon = true;
			while(keepon){
				System.out.println("MAIN MENU");
				System.out.println("---------");
				System.out.println("1. Add Ship");
				System.out.println("2. Add Captain");
				System.out.println("3. Add Cruise");
				System.out.println("4. Book Cruise");
				System.out.println("5. List number of available seats for a given Cruise.");
				System.out.println("6. List total number of repairs per Ship in descending order");
				System.out.println("7. Find total number of passengers with a given status");
				System.out.println("8. < EXIT");
				
				switch (readChoice()){
					case 1: AddShip(esql); break;
					case 2: AddCaptain(esql); break;
					case 3: AddCruise(esql); break;
					case 4: BookCruise(esql); break;
					case 5: ListNumberOfAvailableSeats(esql); break;
					case 6: ListsTotalNumberOfRepairsPerShip(esql); break;
					case 7: FindPassengersCountWithStatus(esql); break;
					case 8: keepon = false; break;
				}
			}
		}catch(Exception e){
			System.err.println (e.getMessage ());
		}finally{
			try{
				if(esql != null) {
					System.out.print("Disconnecting from database...");
					esql.cleanup ();
					System.out.println("Done\n\nBye !");
				}//end if				
			}catch(Exception e){
				// ignored.
			}
		}
	}

	public static int readChoice() {
		int input;
		// returns only if a correct value is given.
		do {
			System.out.print("Please make your choice: ");
			try { // read the integer, parse it and break.
				input = Integer.parseInt(in.readLine());
				break;
			}catch (Exception e) {
				System.out.println("Your input is invalid!");
				continue;
			}//end try
		}while (true);
		return input;
	}//end readChoice

public static void AddShip(DBproject esql) {//1
        try {
            String query = "INSERT INTO Ship (shipID, shipAge, shipSeats, shipMake, shipModel) VALUES (";
            System.out.println("Enter the ship ID:");
            String inputShipID = in.readLine();
            System.out.println("Enter the ship's age:");
            String inputShipAge = in.readLine();
            System.out.println("Enter the number of ship seats:");
            String inputShipSeats = in.readLine();
            System.out.println("Enter the make of the ship:");
            String inputShipMake = in.readLine();
            System.out.println("Enter the ship's model:");
            String inputShipModel = in.readLine();

            query += inputShipID + ",'" + inputShipAge + "','" + inputShipSeats + "','" + inputShipMake + "'," + inputShipModel + "');";
            System.out.println(query);
            esql.executeUpdate(query);

            String temp = "SELECT S.id FROM Ship S  = ";
            temp += inputShipID + ";";
            esql.executeQuery(temp);
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }

        /*int shipID = 0;
        int shipAge = 0;
        int shipSeats = 0;
        String shipMake = " ";
        String shipModel = " ";

        String sql = "SELECT id FROM ship;";
        int id = 0;
        try {
            id = esql.executeQuery(sql) + 1;
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        System.out.println(id);*/
    }
    public static void AddCaptain(DBproject esql) {//2
        try {
            String query = "INSERT INTO Captain (captainID,captainName) VALUES (";
            System.out.println("Enter the captain ID:");
            String captainID = in.readLine();
            System.out.println("Enter the name of the captain:");
            String captainName = in.readLine();

            query += captainID + ",'" + captainName + "');";
            System.out.println(query);
            esql.executeUpdate(query);

            String temp = "SELECT C.name FROM Captain C WHERE C.captainID = " + captainID;
            esql.executeQuery(temp);
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }

    public static void AddCruise(DBproject esql) {//3
            try {
                String query = "INSERT INTO Cruise (cruiseDestination, cruiseDepartureTime, " +
                        "cruiseDepartureDate, cruiseArrivalTime, cruiseArrivalDate, cruiseNumStops, " +
                        "cruiseTicketsSold, cruiseTicketCost, cruiseNumber) VALUES (";
                System.out.println("Enter the cruise destination:");
                String cruiseDestination = in.readLine();
                System.out.println("Enter the cruise departure time:");
                String cruiseDepartureTime = in.readLine();
                System.out.println("Enter the cruise departure date:");
                String cruiseDepartureDate = in.readLine();
                System.out.println("Enter the cruise arrival time:");
                String cruiseArrivalTime = in.readLine();
                System.out.println("Enter the cruise arrival date:");
                String cruiseArrivalDate = in.readLine();
                System.out.println("Enter the cruise number of stops:");
                String cruiseNumStops = in.readLine();
                System.out.println("Enter the number of cruise tickets sold:");
                String cruiseTicketsSold = in.readLine();
                System.out.println("Enter the cruise ticket price:");
                String cruiseTicketCost = in.readLine();
                System.out.println("Enter the cruise number:");
                String cruiseNumber = in.readLine();
                query += cruiseNumber + "," + cruiseTicketsSold + "," + cruiseTicketCost + ",'" + cruiseArrivalTime +
                        "','" + cruiseDepartureDate + "','" + cruiseArrivalTime + "','" + cruiseArrivalDate + "','" +
                        cruiseNumStops + "')";
                System.out.println(query);
                esql.executeUpdate(query);

                String temp = "SELECT C.name FROM Cruise C WHERE C.cruiseNumber = " + cruiseNumber;
                esql.executeQuery(temp);
            }
            catch (Exception e) {
                System.err.println(e.getMessage());
            }
    }

	/**
	 * Take in a cruise number, customerid, and rnum
	 * Do two queries to find the total reservations on the cruise and the total seats on the ship to see if there are any seats left
	 * if there are more  seats on the ship than reserved seats then we book a customer with a reservation with all the appropriate credentials,
	 * if not, then we put them on the waitlist instead using status = "R" or "W".
	 */
	public static void BookCruise(DBproject esql) {//4
		try {
			String status;
			System.out.print("Enter Cruise Number: ");
			int cruisenumber = Integer.parseInt(in.readLine());
			System.out.print("Enter Customer Id: ");
			String customerid = in.readLine();
			System.out.print("Enter A Reservation Number: ");
			String rnum  = in.readLine();

			String query = "SELECT COUNT(rnum) FROM Reservation WHERE cid = " + cruisenumber;
			List<List<String>> queryresult = esql.executeQueryAndReturnResult(query);
			int totalreservations = Integer.parseInt(queryresult.get(0).get(0));

			query = "SELECT S.seats FROM Ship S, CruiseInfo C WHERE C.cruise_id = " + cruisenumber + " AND S.id = C.ship_id";
			queryresult = esql.executeQueryAndReturnResult(query);
			int totalseats = Integer.parseInt(queryresult.get(0).get(0));

			if (totalseats > totalreservations) {
				status = "R";
			}
			else {
				status = "W";
			}
			query = "INSERT INTO Reservation (rnum, ccid, cid, status) VALUES (";
			query += rnum + "," + customerid + "," + cruisenumber + "," + status + ");";
			esql.executeUpdate(query);
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
		}
	}

	/**
	 * Try and catch any errors with running sql queries
	 * Prompt user to give cruisenumber and cruise date and then call a query to find the amount of seats for sold for that cruise.
	 * Take results as a List<List<String>> and store its first element inside reservedseats
	 * Do a query to find the total seats on the ship and store it in total seats from the first element of the nested list.
	 * Find out the amount of free seats using the difference between totalseats and reserved seats and print it out.
	 */

	public static void ListNumberOfAvailableSeats(DBproject esql) {//5
		try {
			// For Cruise number and date, find the number of availalbe seats (i.e. total Ship capacity minus booked seats )
			System.out.print("Enter Cruise Number: ");
			int cruisenumber = Integer.parseInt(in.readLine());
			System.out.print("Enter Cruise Departure Time: ");
			String date = in.readLine();

			String query = "SELECT num_sold FROM Cruise WHERE cnum = " + cruisenumber;
			List<List<String>> queryresult_reserved = esql.executeQueryAndReturnResult(query);
			int reservedseats = Integer.parseInt(queryresult_reserved.get(0).get(0));

			query = "SELECT S.seats FROM Ship S, CruiseInfo C WHERE C.cruise_id = " + cruisenumber + " AND S.id = C.ship_id";
			List<List<String>> queryresult_total = esql.executeQueryAndReturnResult(query);
			int totalseats = Integer.parseInt(queryresult_total.get(0).get(0));

			int freeseats = totalseats - reservedseats;
			System.out.print("Number of Available Seats: " + freeseats);
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
		}
	}

	/**
	 * Try and catch any errors with running sql queries
	 * query the amount of repairs done on each ship and group the queries by ship id
	 * store the results in the List<List<String>> where we have all the ships in descending order on the outer List based on repairs.
	 * The inner list has the ship id in the first element and the number of repairs in the second element so we for loop through the
	 * first List and print out the second list elements accordingly.
	 */
	public static void ListsTotalNumberOfRepairsPerShip(DBproject esql) {//6
		// Count number of repairs per Ships and list them in descending order
		try {
			String query = "SELECT S.id, COUNT(R.rid) FROM Ship S, Repairs R WHERE S.id = R.ship_id GROUP BY S.id ORDER BY Desc COUNT(R.rid);";
			List<List<String>> queryresult = esql.executeQueryAndReturnResult(query);

			for (int i = 0; i < queryresult.size(); i++) {
				System.out.println("Ship: " + queryresult.get(i).get(0) + ", Repairs: " + queryresult.get(i).get(1));
			}
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
		}
	}

	/**
	 * Try and catch any errors with running sql queries
	 * take in a cruisenumber and the status we want to see and run a query to return the amount of that type of status reservations for the cruise
	 * we take the result as a List<List<String>> but only use the first element of both the other and inner list to get our query result.
	 */
	public static void FindPassengersCountWithStatus(DBproject esql) {//7
		// Find how many passengers there are with a status (i.e. W,C,R) and list that number.
		try {
			System.out.print("Enter Cruise Number: ");
			int cruisenumber = Integer.parseInt(in.readLine());
			System.out.print("Enter Passenger Status: ");
			String status = in.readLine();

			String query = "SELECT COUNT(rnum) FROM Reservation WHERE cid = " + cruisenumber + " AND status = " + status + ";";
			List<List<String>> queryresult = esql.executeQueryAndReturnResult(query);
			System.out.println("Cruise has " + queryresult.get(0).get(0) + " people on the " + status + " list.");
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
		}
	}
}
