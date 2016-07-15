// jdbcexp [-opciones] <input_sql_file> <output_txt_file>
// download from query to txt
// this release has not user and password as parameter, todo for next releases
// needs java installed & driver JDBC set, this release is for Oracle, future releases will be as parameter
// Cristóbal Almudéver Gómez - May 2015 - ver 2

import java.sql.* ;
import java.io.* ;
import java.nio.charset.Charset ;
import java.nio.file.* ;
import java.util.List ;


public class jdbcexp {
	public static void main(String[] args) throws IOException {
	
	String fSQL = "" ;
	String fTXT = "" ;
	Boolean skip = true ;
	Boolean fallo = false;
	
	for (int a = 0; a < args.length; a++) {
		if (args[a].equals("-c") ) { // -c Fix € and double space
				skip = false ;
				System.out.println("Fix € and double space");
		}
		else if (args[a].charAt(0) != '-' & fSQL == "") {
				fSQL = args[a].toString() ;
				System.out.println("Processing: " + fSQL);
		}
		else if (args[a].charAt(0) != '-' & fTXT == "") {
				fTXT = args[a].toString() ;
		}	 	
		else {
			System.out.println("ERROR in parameter: " + args[a]) ;
			fallo = true ;
			break;
		}
	}
	
	if (fSQL == "" | fTXT == "" | fallo) {
		System.out.println("Use:") ;
		System.out.println("jdbcexp [-parameters] <input_sql_file> <output_txt_file>") ;
		System.out.println("Parámetros:") ;
		System.out.println("\t-c Fix € and double space to output") ;
		System.exit (-1) ;
	}
	
	
	// driver connection JDBC to BBDD
	try {
		Class.forName("oracle.jdbc.driver.OracleDriver");
	}
	catch (ClassNotFoundException e) {
		System.err.println (e) ;
		System.exit (-1) ;
	}
	System.out.println("Driver OK.");
	
	// conexión a la BBDD
	try {
		Connection connection = DriverManager.getConnection(
			"jdbc:oracle:thin:@//IP:PORT:DDBB",
			"USER",
			"PASSWORD"
		);
		System.out.println("DDBB OK.");

		Path fichero = Paths.get("", fSQL);
		
		Charset charset = Charset.forName("UTF-8");
		
		// Carga de la Query a procesar
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

		// Ejecución del la Query, carga en el RS 
		Statement statement = connection.createStatement () ;
		ResultSet rs = statement.executeQuery (query) ;
		
		ResultSetMetaData rsmd = rs.getMetaData();
		
		FileWriter fw =  new FileWriter(fTXT);
		int rows = 0 ;
		String tstring = null ;
		while ( rs.next () ) {
			// Send to file
			rows++;
			for (int i = 1; i <= rsmd.getColumnCount(); i++) {
				tstring = rs.getString(i) ;
				if (!skip) {
					//solucion al problema euro
					tstring = (tstring != null)?tstring.replaceAll("","€"):"" ; 
					//solucion espacios dobles
					while (tstring.contains("  ")) {
						tstring = (tstring != null)?tstring.replaceAll("  "," "):"" ;
					} 
				}
				if (i == rsmd.getColumnCount()) 
					{fw.write (((rs.getString (i) == null)?"":tstring) + "|\r\n"); } 
				else 
					{fw.write (((rs.getString (i) == null)?"":tstring) + "|"); }
			}
		if (rows % 1000 == 0) { System.out.print("·") ; }
		}	
		connection.close () ;
		
		fw.close();
		System.out.println("·\n" + rows + " Records.\n");

	}
	catch (java.sql.SQLException e) {
		System.err.println (e) ;
		System.exit (-1) ;
	}
	}
}
