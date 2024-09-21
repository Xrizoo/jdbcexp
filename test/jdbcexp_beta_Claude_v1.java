// jdbcexp [-options] <input.sql> <output.csv>
// Export/Unload querys to CSV files
// Needs java and JDBC driver to be installed on running system.
// Cristóbal Almudéver Gómez - Sep. 2024 - ver 3.3b - Claude optimized version.

import java.sql.*;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.List;

public class jdbcexp {
    public static void main(String[] args) {
        String fSQL = "";
        String fTXT = "";
        String vURL = "";
        String vDRIVER = "";
        String vUSER = "";
        String vPASSWORD = "";
        String vSEPARATOR = "|";
        boolean skip = true;
        boolean addToTarget = false;

        for (int a = 0; a < args.length; a++) {
            switch (args[a]) {
                case "-?":
                    printHelp();
                    return;
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
                    vSEPARATOR = getSeparator(args[++a]);
                    System.out.println("Column char separation: " + args[a] + " set");
                    break;
                default:
                    if (args[a].charAt(0) != '-') {
                        if (fSQL.isEmpty()) {
                            fSQL = args[a];
                            System.out.print("Processing: " + fSQL);
                        } else if (fTXT.isEmpty()) {
                            fTXT = args[a];
                            System.out.println((addToTarget ? " >> " : " > ") + fTXT);
                        }
                    } else {
                        System.out.println("Parameter ERROR: " + args[a]);
                        printHelp();
                        return;
                    }
            }
        }

        if (fSQL.isEmpty() || fTXT.isEmpty()) {
            printHelp();
            return;
        }

        try {
            Class.forName(vDRIVER);
            System.out.println("Driver OK.");

            try (Connection connection = DriverManager.getConnection(vURL, vUSER, vPASSWORD);
                 Statement statement = connection.createStatement()) {

                System.out.println("DDBB OK.");

                String query = Files.readString(Paths.get(fSQL), StandardCharsets.UTF_8);

                try (ResultSet rs = statement.executeQuery(query);
                     FileWriter fw = new FileWriter(fTXT, addToTarget)) {

                    ResultSetMetaData rsmd = rs.getMetaData();
                    int columnCount = rsmd.getColumnCount();
                    int records = 0;

                    while (rs.next()) {
                        records++;
                        for (int i = 1; i <= columnCount; i++) {
                            String value = rs.getString(i);
                            if (!skip && value != null) {
                                value = value.replace("", "€").replaceAll("\\s+", " ");
                            }
                            fw.write((value == null ? "" : value) + (i == columnCount ? vSEPARATOR + "\r\n" : vSEPARATOR));
                        }
                        if (records % 1000 == 0) {
                            System.out.print("·");
                        }
                    }
                    System.out.println("·\n" + records + " Exported records.\n");
                }
            }
        } catch (ClassNotFoundException | SQLException | IOException e) {
            System.err.println(e);
            System.exit(-1);
        }
    }

    private static String getSeparator(String arg) {
        return switch (arg) {
            case "t" -> "\t";
            case "r" -> "\r";
            case "n" -> "\n";
            default -> arg;
        };
    }

    private static void printHelp() {
        System.out.println("\nJDBCEXP v3.3");
        System.out.println("Export data from DDBB with JDBC by Cristobal Almudever - May.2018");
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
