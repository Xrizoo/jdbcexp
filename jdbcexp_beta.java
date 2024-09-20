
// jdbcexp [-options] <input.sql> <output.csv>
// Export/Unload queries to CSV files
// Needs java and JDBC driver to be installed on the running system.
// Cristóbal Almudéver Gómez - Sep 2024 - ver 3.3b

import java.sql.*;
import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.*;
import java.util.*;

public class jdbcexp_beta {
    public static void main(String[] args) throws IOException {

        String fSQL = "";
        String fTXT = "";
        String vURL = "";
        String vDRIVER = "";
        String vUSER = "";
        String vPASSWORD = "";
        String vSEPARATOR = "|";
        boolean skip = true;
        boolean fallo = false;
        boolean help = false;
        boolean addToTarget = false;

        // Handling parameters
        Map<String, String> params = new HashMap<>();
        for (int a = 0; a < args.length; a++) {
            switch (args[a]) {
                case "-c":
                    vURL = args[++a];
                    break;
                case "-d":
                    vDRIVER = args[++a];
                    break;
                case "-u":
                    vUSER = args[++a];
                    break;
                case "-p":
                    vPASSWORD = args[++a];
                    break;
                case "-s":
                    vSEPARATOR = args[++a];
                    if (vSEPARATOR.equals("t")) vSEPARATOR = "\t";
                    if (vSEPARATOR.equals("r")) vSEPARATOR = "\r";
                    if (vSEPARATOR.equals("n")) vSEPARATOR = "\n";
                    break;
                case "-a":
                    addToTarget = true;
                    break;
                case "-f":
                    skip = false;
                    break;
                case "-?":
                    help = true;
                    break;
                default:
                    if (!args[a].startsWith("-")) {
                        if (fSQL.isEmpty()) {
                            fSQL = args[a];
                        } else {
                            fTXT = args[a];
                        }
                    } else {
                        System.err.println("Unknown parameter: " + args[a]);
                        fallo = true;
                        break;
                    }
            }
        }

        if (help || fSQL.isEmpty() || fTXT.isEmpty() || fallo) {
            showHelp();
            System.exit(-1);
        }

        // JDBC driver connection
        try {
            Class.forName(vDRIVER);
        } catch (ClassNotFoundException e) {
            System.err.println(e);
            System.exit(-1);
        }
        System.out.println("Driver OK.");

        // Database connection
        try (Connection connection = DriverManager.getConnection(vURL, vUSER, vPASSWORD);
             Statement statement = connection.createStatement()) {
            System.out.println("Database OK.");

            // Load query from file
            Path sqlFile = Paths.get(fSQL);
            Charset charset = Charset.forName("UTF-8");
            StringBuilder queryBuilder = new StringBuilder();

            try {
                List<String> lines = Files.readAllLines(sqlFile, charset);
                for (String line : lines) {
                    queryBuilder.append(" ").append(line);
                }
            } catch (IOException e) {
                System.err.println(e);
                System.exit(-1);
            }

            String query = queryBuilder.toString();
            ResultSet rs = statement.executeQuery(query);
            ResultSetMetaData rsmd = rs.getMetaData();

            try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(fTXT), Charset.forName("UTF-8"),
                    addToTarget ? StandardOpenOption.APPEND : StandardOpenOption.CREATE)) {
                int records = 0;
                while (rs.next()) {
                    for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                        String value = rs.getString(i);
                        if (!skip) {
                            if (value != null) {
                                value = value.replaceAll("", "€").replaceAll("  ", " ");
                            }
                        }
                        writer.write((value == null ? "" : value));
                        if (i < rsmd.getColumnCount()) {
                            writer.write(vSEPARATOR);
                        }
                    }
                    writer.newLine();
                    if (++records % 1000 == 0) {
                        System.out.print("·");
                    }
                }
                System.out.println("·\n" + records + " records exported.");
            }
        } catch (SQLException e) {
            System.err.println(e);
            System.exit(-1);
        }
    }

    private static void showHelp() {
		System.out.println();
		System.out.println("JDBCEXP v3.3b");
		System.out.println("Export data from DDBB with JDBC by Cristobal Almudever - Sep.2024") ;
		System.out.println("Use:") ;
		System.out.println("\tjdbcexp [-parameters] <input.sql> <output.csv>") ;
		System.out.println("Parameters:") ;
		System.out.println("\t-c <URL-JDBC-Connection-string>") ;
		System.out.println("\t\tie: -c jdbc:oracle:thin:@127.0.0.1:1539:DDBB") ;
		System.out.println("\t-d <JDBC-driver-string>") ;
		System.out.println("\t\tie: -d oracle.jdbc.driver.OracleDriver") ;		
		System.out.println("\t-u <User>") ;		
		System.out.println("\t-p <Password>") ;
		System.out.println("\t-s <separator-column-char> -> default is |") ;
		System.out.println("\t\tFor special chars, preceded by \\") ;		
		System.out.println("\t-f Fix character € and double space") ;		
		System.out.println("\t-a Add result to output file, not overwrite") ;		
		System.exit (-1) ;
    }
}
