// jdbcexp [-options] <input.sql> <output.csv>
// Export/Unload querys to CSV files
// Needs java and JDBC driver to be installed on running system.
// Cristóbal Almudéver Gómez - May 2018 - ver 3.2

import java.sql.* ;
import java.io.* ;
import java.nio.charset.Charset ;
import java.nio.file.* ;
import java.util.List ;
import java.math.* ; // for BigDecimal and BigInteger support

public class jdbcexp {
	public static void main(String[] args) throws IOException {
	
	String fSQL = "" ;
	String fTXT = "" ;
	String vURL = "" ;
	String vDRIVER = "" ;	
	String vUSER = "" ;
	String vPASSWORD = "" ;		
	String vSEPARATOR = "|";
	Boolean skip = true;
	Boolean fallo = false;
	Boolean help = false;
	Boolean addToTarget = false;
		
	System.out.println("RUNNING...");
	for (int a = 0; a < args.length; a++) {
		if (args[a].equals("-?") ) { // -? Help
				help = true ;
				System.out.println("HELP requested.") ;
				break;
		}	
		if (args[a].equals("-f") ) { // -f Fix € and double space
				skip = false ;
				System.out.println("Fix € symbol and double space");
		}
		if (args[a].equals("-a") ) { // -a Add result to the existing file (create new file if not)
				addToTarget = true ;
				System.out.println("Add to output file (create new one if not exist)");
		}		
		else if (args[a].startsWith("-c") ) { // -c URL connection
				a++;
				vURL = args[a];
				System.out.println("URL: " + vURL + " passed");
		}
		else if (args[a].startsWith("-d") ) { // -c Driver connection
				a++;
				vDRIVER = args[a];
				System.out.println("Driver: " + vDRIVER + " passed");
		}
		else if (args[a].startsWith("-u") ) { 
				a++;
				vUSER = args[a];
				System.out.println("User: " + vUSER + " passed");
		}
		else if (args[a].startsWith("-p") ) { 
				a++;
				vPASSWORD = args[a];
				System.out.println("Password: *** passed");
		}
		else if (args[a].startsWith("-s") ) { 
				a++;
				vSEPARATOR = args[a];
				if (vSEPARATOR.startsWith("t")) {vSEPARATOR = "\t";}; // tab character recognition
				if (vSEPARATOR.startsWith("r")) {vSEPARATOR = "\r";}; // return character recognition
				if (vSEPARATOR.startsWith("n")) {vSEPARATOR = "\n";}; // newline character recognition
				System.out.println("Column char separation: " + args[a] + " set");
		}
		else if (args[a].charAt(0) != '-' & fSQL == "") {
				fSQL = args[a].toString() ;
				System.out.print("Processing: " + fSQL);
		}
		else if (args[a].charAt(0) != '-' & fTXT == "") {
				fTXT = args[a].toString() ;
				System.out.println(((addToTarget) ? " -+> " : " -> ") + fTXT);
		}	 	
		else {
			System.out.println("Parameter ERROR: " + args[a]) ;
			fallo = true ;
			break;
		}
	}
	
	if (fSQL == "" | fTXT == "" | fallo | help ) {
		System.out.println();
		System.out.println("JDBCEXP v3.2");
		System.out.println("Export data from DDBB with JDBC by Cristobal Almudever - May.2018") ;
		System.out.println("Use:") ;
		System.out.println("\tjdbcexp [-parameters] <input.sql> <output.txt>") ;
		System.out.println("Parameters:") ;
		System.out.println("\t-c <URL-JDBC-Connection-string>") ;
		System.out.println("\t\tie: -c jdbc:oracle:thin:@127.0.0.1:1539:DDBB") ;
		System.out.println("\t-d <JDBC-driver-string>") ;
		System.out.println("\t\tie: -d oracle.jdbc.driver.OracleDriver") ;		
		System.out.println("\t-u <User>") ;		
		System.out.println("\t-p <Password>") ;
		System.out.println("\t-s <Column-char-separation> -> default is |") ;
		System.out.println("\t\tFor special chars, preceded by \\") ;		
		System.out.println("\t-f Fix character € and double space") ;		
		System.out.println("\t-a Add result to output file, not overwrite") ;		
		System.exit (-1) ;
	}
	
	
	// Driver JDBC connection to BBDD
	try {
		Class.forName(vDRIVER);
	}
	catch (ClassNotFoundException e) {
		System.err.println (e) ;
		System.exit (-1) ;
	}
	System.out.println("Driver OK.");
	
	// BBDD connection
	try {
		Connection connection = DriverManager.getConnection(
			vURL,
			vUSER,
			vPASSWORD
		);
		System.out.println("DDBB OK.");

		Path fichero = Paths.get("", fSQL);
		
		Charset charset = Charset.forName("UTF-8");
		
		// Load query to process
		String query = "" ;
		try {
			List<String> Lquery = Files.readAllLines(fichero, charset) ;
			for (int i = 0; i < Lquery.size(); i++) {
				query = query + " " + Lquery.get(i);
			}
		}
		catch (IOException e) {
			System.err.println (e) ;
			System.exit (-1) ;
		}

		// Query execution, load to RS
		Statement statement = connection.createStatement () ;
		ResultSet rs = statement.executeQuery (query) ;
		
		ResultSetMetaData rsmd = rs.getMetaData();
		
		FileWriter fw =  new FileWriter(fTXT, addToTarget);
		int records = 0 ;
		String tstring = null ;
		while ( rs.next () ) {
			// Send to a file
			records++;
			for (int i = 1; i <= rsmd.getColumnCount(); i++) {
				tstring = rs.getString(i) ;
				if (!skip) {
					// euro fix symbol
					tstring = (tstring != null)?tstring.replaceAll("","€"):"" ; 
					// double space fix
					while (tstring.contains("  ")) {
						tstring = (tstring != null)?tstring.replaceAll("  "," "):"" ;
					} 
				}
				if (i == rsmd.getColumnCount()) 
					{fw.write (((rs.getString (i) == null)?"":tstring) + vSEPARATOR + "\r\n"); } 
				else 
					{fw.write (((rs.getString (i) == null)?"":tstring) + vSEPARATOR ); }
			}
		if (records % 1000 == 0) { System.out.print("·") ; } // on every 1000 records, display one dot
		}	
		connection.close () ;
		
		fw.close();
		System.out.println("·\n" + records + " Exported records.\n");

	}
	catch (java.sql.SQLException e) {
		System.err.println (e) ;
		System.exit (-1) ;
	}
	}
}
