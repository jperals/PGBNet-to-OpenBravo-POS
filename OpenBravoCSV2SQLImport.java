import au.com.bytecode.opencsv.CSVReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
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
    private static ArrayList<String[]> productsWithoutCode = new ArrayList<String[]>();

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
            System.out.println("Total: " + productCount);
            System.out.println(productsWithoutCode.size() + " products had a barcode full of zeros, and an incremental number was used instead.");
        }
    }
    
    private static void readAndImportCSV() throws IOException, SQLException {
		CSVReader reader = new CSVReader(new FileReader(csvFileName));
        String[] line;
        try {            
            while ((line = reader.readNext()) != null) {
                insertProduct(line);
            }
            int nProductsWithoutCode = productsWithoutCode.size();
            System.out.println("Finished inserting products with valid barcode. Now inserting " + nProductsWithoutCode + " products which barcode was \"0000000000000\".");
            for (int i=0; i < nProductsWithoutCode; i++) {
                String[] productProperties = productsWithoutCode.get(i);
                productProperties[26] = String.format("%013d", i);
                insertProduct(productProperties);
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
        
    private static void insertProduct(String[] productProperties) throws SQLException {
        String barcode = productProperties[26];
        if(barcode.equals("0000000000000") || barcode.equals("") {
            productsWithoutCode.add(productProperties);
        }
        else {
            String productId = UUID.randomUUID().toString();
            String productName = productProperties[25].replace("\"", "\\\"");
            int priceBuy = (Integer.parseInt(productProperties[27])/100);
            int priceSell = (Integer.parseInt(productProperties[30])/100);
            String SQLQuery = "INSERT INTO PRODUCTS (ID, REFERENCE, CODE, NAME, PRICEBUY, PRICESELL, CATEGORY, TAXCAT) VALUES (\"" + productId + "\", \"" + productProperties[24] + "\", \"" + barcode + "\", \"" + productName + "\", " + priceBuy + ", " + priceSell + ", \"000\", \"001\")";
            try {
                rs = stmt.executeUpdate(SQLQuery);
                if (rs > 0) {
                    productsAdded++;
                    System.out.println("Product " + productCount + " added: \"" + productProperties[25] + "\"");
                }
                else {
                    productsSkipped++;
                    System.out.println("WARNING: Product not added: " + productProperties[25]);
                }
                rs = stmt.executeUpdate("INSERT INTO PRODUCTS_CAT (PRODUCT) VALUES (\"" + productId + "\")");
                if (rs > 0) {
                    System.out.println("Product added to catalog.");
                }
                else {
                    System.out.println("Product not added to catalog.");
                }
            }
            catch (SQLException ex) {
                System.out.println("SQLException: " + ex.getMessage());
                System.out.println("SQLState: " + ex.getSQLState());
                System.out.println("VendorError: " + ex.getErrorCode());
                System.out.println("Query: " + SQLQuery);
                productsSkipped++;
            }
            finally {
                productCount++;
            }
        }
    }
}
