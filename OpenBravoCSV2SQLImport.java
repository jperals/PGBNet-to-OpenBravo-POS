import au.com.bytecode.opencsv.CSVReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.Properties;
import java.util.UUID;

public class OpenBravoCSV2SQLImport
{
    private static final String configFileName = "config.properties";
    private static final String csvFileName = "plu1.csv";
    private static final String dbClassName = "com.mysql.jdbc.Driver";
    private static Statement stmt = null;
    private static int rs;
    private static int productCount = 0;
    private static int productsAdded = 0;
    private static int productsSkipped = 0;

    public static void main(String[] args) throws ClassNotFoundException, IOException, SQLException {
        connectAndStart();
    }
    
    public static void connectAndStart() throws ClassNotFoundException, IOException, SQLException {
        Class.forName(dbClassName);
        Properties properties = new Properties();
        try {
    		properties.load(new FileInputStream(configFileName));
            String connectionURL = properties.getProperty("database");
            Connection connection = DriverManager.getConnection(connectionURL,properties);
            System.out.println("Connected to the database");
            stmt = connection.createStatement();
            readAndImportCSV();
            connection.close();
    	} catch (IOException ex) {
    		ex.printStackTrace();
        }
        finally {
            if (stmt != null) {
                try {
                    stmt.close();
                }
                catch (SQLException sqlEx) {
                    System.out.println("SQLException: " + sqlEx.getMessage());
                }
                stmt = null;
            }
            System.out.println("Done.");
            System.out.println("Products added: " + productsAdded);
            System.out.println("Products skipped: " + productsSkipped);
            System.out.println("Total " + productCount);
        }
    }
    
    private static void readAndImportCSV() throws IOException, SQLException {
		CSVReader reader = new CSVReader(new FileReader(csvFileName));
        String[] line;
        try {            
            while ((line = reader.readNext()) != null) {
                insertProduct(line);
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    private static void insertProduct(String[] productProperties) throws SQLException {
        String productId = UUID.randomUUID().toString();
        String productName = productProperties[25].replace("\"", "\\\"");
        int priceBuy = (Integer.parseInt(productProperties[27])/100);
        int priceSell = (Integer.parseInt(productProperties[30])/100);
        String SQLQuery = "INSERT INTO PRODUCTS (ID, REFERENCE, CODE, NAME, PRICEBUY, PRICESELL, CATEGORY, TAXCAT) VALUES (\"" + productId + "\", \"" + productProperties[24] + "\", \"" + productProperties[26] + "\", \"" + productName + "\", " + priceBuy + ", " + priceSell + ", \"000\", \"001\")";
        try {
            //System.out.println(SQLQuery);
            rs = stmt.executeUpdate(SQLQuery);
            rs = stmt.executeUpdate("INSERT INTO PRODUCTS_CAT (PRODUCT) VALUES (\"" + productId + "\")");
        }
        catch (SQLException ex) {
            System.out.println("SQLException: " + ex.getMessage());
            System.out.println("SQLState: " + ex.getSQLState());
            System.out.println("VendorError: " + ex.getErrorCode());
            System.out.println("Query: " + SQLQuery);
        }
        finally {
            productCount++;
            if (rs > 0) {
                productsAdded++;
                System.out.println("Product " + productCount + " successfully added: " + productProperties[25]);
            }
            else {
                productsSkipped++;
                System.out.println("WARNING: Product not added: " + productProperties[25]);
            }
        }
    }
}
