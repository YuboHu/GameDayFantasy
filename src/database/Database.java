package database;

import java.sql.*;

public class Database
{

    // init database constants
    private static final String DATABASE_DRIVER = "com.mysql.jdbc.Driver";
    private static final String DATABASE_URL_BASE = "jdbc:mysql://localhost:3306/fantasyplayer";

    private static final String USERNAME = "george";
    private static final String PASSWORD = "1234";

    private static final String CREATETABLE =
        "create table if not exists performance(PID int NOT NULL,Name varchar(255) NOT NULL,Date char(8) NOT NULL,Team char(3) NOT NULL,Oppo char(3) NOT NULL,Home boolean NOT NULL,GID char(16) NOT NULL,Start boolean NOT NULL,Min decimal(3,1) NOT NULL,GP boolean NOT NULL,GameDayPoints decimal(5,1) NOT NULL,FDSalary int, DKSalary int,Pos int NOT NULL,PRIMARY KEY (GID,PID));";
    private static final String CREATEPREDICTIONTABLE =
        "CREATE TABLE if not exists Predictions ( PID int NOT NULL, Pos int NOT NULL, Name varchar(255) NOT NULL, Prediction decimal(10,5) NOT NULL, GDSalary int, FDSalary int, DKSalary int, PRIMARY KEY (PID) );";
    // init connection object
    private Connection connection;

    private boolean tableCreated = false;

    public Database()
    {
        connection = null;
    }

    // connect database
    public void connect() throws Exception
    {
        if (connection == null)
        {
            try
            {
                Class.forName(DATABASE_DRIVER);
                connection = DriverManager.getConnection(DATABASE_URL_BASE, USERNAME, PASSWORD);
            }
            catch (ClassNotFoundException | SQLException e)
            {
                e.printStackTrace();
            }
        }
        if (!tableCreated)
        {
            createTable(CREATETABLE);
            createTable(CREATEPREDICTIONTABLE);
            createTable("Delete from predictions");
            tableCreated = true;
        }
    }

    public ResultSet execQuery(String query) throws SQLException
    {
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery(query);
        return rs;
    }

    public void createTable(String query) throws SQLException
    {
        Statement stmt = connection.createStatement();
        stmt.executeUpdate(query);
    }

    public PreparedStatement prepareStatement(String query) throws SQLException
    {
        return connection.prepareStatement(query);
    }

    // disconnect database
    public void disconnect()
    {
        if (connection != null)
        {
            try
            {
                connection.close();
                connection = null;
            }
            catch (SQLException e)
            {
                e.printStackTrace();
            }
        }
    }

}
