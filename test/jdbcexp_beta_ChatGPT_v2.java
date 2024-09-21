// jdbcexp [-options] <input.sql> <output.csv>
// Export/Unload querys to CSV files
// Needs java and JDBC driver to be installed on running system.
// Cristóbal Almudéver Gómez - Sep. 2024 - ver 3.3b - ChatGPT optimized version.

import java.sql.*;
import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.*;
import java.util.List;

public class jdbcexp {
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

        System.out.println("RUNNING...");
        for (int a = 0; a < args.length; a++) {
            switch (args[a]) {
                case "-?":
                    help = true;
                    System.out.println("HELP requested.");
                    break;
                case "-f":
                    skip = false;
                    System.out.println("Fix € symbol and double space");
                    break;
                case "-a":
                    addToTarget = true;
                    System.out.println("Add to output file (create new one if not exist)");
                    break;
                case "-c":
                    vURL = args[++a];
                    System.out.println("URL: " + vURL + " passed");
                    break;
                case "-d":
                    vDRIVER = args[++a];
                    System.out.println("Driver: " + vDRIVER + " passed");
                    break;
                case "-u":
                    vUSER = args[++a];
                    System.out.println("User: " + vUSER + " passed");
                    break;
                case "-p":
                    vPASSWORD = args[++a];
                    System.out.println("Password: *** passed");
                    break;
                case "-s":
                    vSEPARATOR = args[++a];
                    if (vSEPARATOR.equals("t")) vSEPARATOR = "\t";
                    if (vSEPARATOR.equals("r")) vSEPARATOR = "\r";
                    if (vSEPARATOR.equals("n")) vSEPARATOR = "\n";
                    System.out.println("Column char separation: " + vSEPARATOR + " set");
                    break;
                default:
                    if (fSQL.isEmpty()) {
                        fSQL = args[a];
                        System.out.print("Processing: " + fSQL);
                    } else if (fTXT.isEmpty()) {
                        fTXT = args[a];
                        System.out.println((addToTarget ? " >> " : " > ") + fTXT);
                    } else {
                        System.out.println("Parameter ERROR: " + args[a]);
                        fallo = true;
                        break;
                    }
            }
        }

        if (fSQL.isEmpty() || fTXT.isEmpty() || fallo || help) {
            printHelp();
            System.exit(-1);
        }

        try {
            Class.forName(vDRIVER);
        } catch (ClassNotFoundException e) {
            System.err.println(e);
            System.exit(-1);
        }
        System.out.println("Driver OK.");

        try (Connection connection = DriverManager.getConnection(vURL, vUSER, vPASSWORD);
             FileWriter fw = new FileWriter(fTXT, addToTarget);
             Statement statement = connection.createStatement()) {

            System.out.println("DDBB OK.");
            Path fichero = Paths.get("", fSQL);
            Charset charset = Charset.forName("UTF-8");
            StringBuilder queryBuilder = new StringBuilder();

            List<String> Lquery = Files.readAllLines(fichero, charset);
            for (String line : Lquery) {
                queryBuilder.append(" ").append(line);
            }
            String query = queryBuilder.toString();

            try (ResultSet rs = statement.executeQuery(query)) {
                ResultSetMetaData rsmd = rs.getMetaData();
                int records = 0;

                while (rs.next()) {
                    records++;
                    StringBuilder row = new StringBuilder();
                    for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                        String tstring = rs.getString(i) != null ? rs.getString(i) : "";
                        if (!skip) {
                            tstring = tstring.replace("", "€").replaceAll("  ", " ");
                        }
                        row.append(tstring).append(i == rsmd.getColumnCount() ? "\r\n" : vSEPARATOR);
                    }
                    fw.write(row.toString());

                    if (records % 1000 == 0) {
                        System.out.print("·");
                    }
                }
                System.out.println("·\n" + records + " Exported records.\n");
            }

        } catch (SQLException | IOException e) {
            System.err.println(e);
            System.exit(-1);
        }
    }

    private static void printHelp() {
        System.out.println();
        System.out.println("JDBCEXP v3.3");
        System.out.println("Export data from DDBB with JDBC by Cristobal Almudever - Sep.2024");
        System.out.println("Use:");
        System.out.println("\tjdbcexp [-parameters] <input.sql> <output.csv>");
        System.out.println("Parameters:");
        System.out.println("\t-c <URL-JDBC-Connection-string>");
        System.out.println("\t\tie: -c jdbc:oracle:thin:@127.0.0.1:1539:DDBB");
        System.out.println("\t-d <JDBC-driver-string>");
        System.out.println("\t\tie: -d oracle.jdbc.driver.OracleDriver");
        System.out.println("\t-u <User>");
        System.out.println("\t-p <Password>");
        System.out.println("\t-s <separator-column-char> -> default is |");
        System.out.println("\t\tFor special chars, preceded by \\");
        System.out.println("\t-f Fix character € and double space");
        System.out.println("\t-a Add result to output file, not overwrite");
    }
}

