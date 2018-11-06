import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Scanner;

public class index {

	private static Scanner scanner;
	final static String driver = "com.mysql.cj.jdbc.Driver";
	final static String url = "jdbc:mysql://localhost/myDB";
	final static String username = "root";
	final static String password = "root";
	final static String Table = "flight";
	Connection con = null;
	
	public index() {

        try {

            System.out.print("  Loading JDBC Driver  -> " + driver + "\n");
            Class.forName(driver).newInstance();

            System.out.print("  Connecting to        -> " + url + "\n");
            this.con = DriverManager.getConnection(url, username, password);
            System.out.print("  Connected as         -> " + username + "\n");

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }
	
	public static Connection getConnection() throws Exception {
	    
	    Class.forName(driver);
	    Connection conn = DriverManager.getConnection(url, username, password);
	    return conn;
}
	
	public void create() throws FileNotFoundException {

        Statement stmt = null;

        try {
            stmt = con.createStatement();
            
            System.out.print("  Dropping Table: " + Table + "\n");
            stmt.executeUpdate("DROP TABLE " + Table);

            System.out.print("    - Dropped Table...\n");
            
            System.out.print("  Closing Statement...\n");
            stmt.close();

        } catch (SQLException e) {
            System.out.print("    - Table " + Table + " did not exist.\n");
        }


        try {

            stmt = con.createStatement();

            System.out.print("  Creating Table: " + Table + "\n");
           stmt.executeUpdate("CREATE TABLE flight (Airline_Name VARCHAR(10), Origin VARCHAR(15),"
           		+ " Origin_State VARCHAR(15), Origin_Airport_Code VARCHAR(3), "
           				+ "Destination VARCHAR(15), Destination_State VARCHAR(15), "
           						+ "Destination_Airport_Code VARCHAR(3), Departure_Date DATE, "
           					+	"Price FLOAT, Seats_Available INTEGER)");

            System.out.print("    - Created Table...\n");

            System.out.print("  Closing Statement...\n");
            stmt.close();

        } catch (SQLException e) {
            e.printStackTrace();
           
        }

    }
	
	public void update() throws Exception {
		
		String[] details = null;
	   	try {   
		File fileName = new File("src//Flight.txt");
	   	 PreparedStatement query = null;
	   	Connection conn = getConnection();
	   	 query = conn.prepareStatement("INSERT INTO " + Table + "(Airline_Name, Origin, Origin_State, Origin_Airport_Code,"
	   	 		+ "Destination, Destination_State, Destination_Airport_Code, Departure_Date, "
	   	 		+ "Price, Seats_Available) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"); 
	   	 
	   	PrintStream orErr = System.err;
		PrintStream fiErr = new PrintStream("src//Output.txt");
		System.setErr(fiErr);
	   	 
	   	    scanner = new Scanner(fileName);
          	//read the file line by line
          while (scanner.hasNextLine()) {
  	   	      String line = scanner.nextLine();
        	   details = line.split("[|]");
        	   
         	  if (new SimpleDateFormat("MM/dd/yyyy").parse(details[7]).before(new Date())) 
         	  { System.err.println("Date is from past - " + details[7]);
         	 orErr.println("Date is from past - " + details[7]);
         	 continue;}
         	  
         	  else if(details[0].equalsIgnoreCase("Delta") || details[0].equalsIgnoreCase("United") || details[0].equalsIgnoreCase("Frontier") || details[0].equalsIgnoreCase("American"))
        	   {  query.setString(1, details[0]);   }
         	  
        	   else  { System.err.println("Airline not allowed - " + details[0]); 
        	   orErr.println("Airline not allowed - " + details[0]);
        			   continue;}
          
        	  query.setString(2, details[1]);  
        	  query.setString(3, details[2]);     
        	  query.setString(4, details[3]);      
        	  query.setString(5, details[4]);       
        	  query.setString(6, details[5]);      
        	  query.setString(7, details[6]);   
        	  
        	  SimpleDateFormat format = new SimpleDateFormat( "MM/dd/yyyy" );
        	  java.util.Date myDate = format.parse( details[7] ); 
        	  java.sql.Date sqlDate = new java.sql.Date( myDate.getTime() );
        	  query.setDate(8, sqlDate); 
        	  query.setString(9, details[8]); 
        	  query.setString(10, details[9]);
        	  
        	  query.executeUpdate();
        	  
          }
          
          PreparedStatement query1 = conn.prepareStatement("SELECT Origin, Destination, Departure_date FROM " + Table + " ORDER BY Price DESC;");
          ResultSet result = query1.executeQuery();
          
          System.out.println("Origin Destination Deaprture_Date");
          while(result.next())
          {
              System.out.println(result.getString(1)+" "+result.getString(2)+" "+result.getDate(3));

          } 
          
          System.setErr(orErr); 
        
	   	} catch (FileNotFoundException e) {         
            e.printStackTrace();
            
        }
	}
	
	public static void main(String[] argv) throws Exception {
		
		index obj = new index();
		obj.create();
	    obj.update();
	}
	
}