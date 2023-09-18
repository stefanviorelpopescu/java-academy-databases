package org.example;

import java.lang.reflect.InvocationTargetException;
import java.sql.*;

public class Main {
    public static void main(String[] args) throws SQLException {
        System.out.println("Hello world!");

        connectToDb();
        selectUsingStatement();
        selectUsingPreparedStatement();
        jdbcTransactionAttempt();
    }

    private static void jdbcTransactionAttempt() throws SQLException {

        Connection connection = getConnection();
        try (connection) {
            if (connection == null)
                return;
            connection.setAutoCommit(false);

            PreparedStatement preparedStatement = connection.prepareStatement("insert into authors values (?,?,?,?)");

            insertAuthor(preparedStatement, 4, "GC", 57, "0766123456");
            insertAuthor(preparedStatement, 5, "GCC", 54, "0766123457");
            insertAuthor(preparedStatement, 6, "CTP", 50, "0766123458");

            connection.commit();
            connection.setAutoCommit(true);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    private static void insertAuthor(PreparedStatement preparedStatement,
                                     int id,
                                     String name,
                                     int age,
                                     String phone)
            throws SQLException {
        preparedStatement.setInt(1, id);
        preparedStatement.setString(2, name);
        preparedStatement.setInt(3, age);
        preparedStatement.setString(4, phone);
        preparedStatement.executeUpdate();
    }

    private static void selectUsingPreparedStatement() {

        Connection connection = getConnection();
        if (connection == null)
            return;
        PreparedStatement ps = null;
        try {
            ps = connection.prepareStatement("select * from authors where age > ?");
            ps.setInt(1, 70);
            ResultSet resultSet = ps.executeQuery();
            displayResults(resultSet);
        } catch (SQLException e) {
            System.err.println("Cannot insert author: " + e.getMessage());
        } finally {
            if (ps != null) try { ps.close(); } catch (SQLException e) { }
        }
    }

    private static void selectUsingStatement() {

        try (Connection connection = getConnection()) {
            if (connection == null) return;
            Statement statement = null;
            ResultSet resultSet = null;
            statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
            resultSet = statement.executeQuery("select * from authors"); // plain SQL statement
            displayResults(resultSet);
        } catch (SQLException e) {
            System.err.println("Cannot execute query: " + e.getMessage());
        }
    }

    private static void displayResults(ResultSet resultSet) throws SQLException {
        final String format = "%20s%20s%12s\n";
        boolean hasResults = resultSet.next(); // position on the first line (initially it’s at -1)
        if (hasResults) {
            System.out.format(format, "Name", "Age", "Phone");
            do {
                System.out.format(format, resultSet.getString("name"),
                        resultSet.getString("age"),
                        resultSet.getString("phone"));
            } while (resultSet.next());
        } else {
            System.out.println("No results");
        }
    }

    private static Connection getConnection() {
        DriverManager.setLoginTimeout(60); // wait 1 min; optional: DB may be busy, good to set a higher timeout
        try {
            String url = "jdbc:" +
                    "postgresql" + // “mysql” / “db2” / “mssql” / “oracle” / ...
                    "://" +
                    "localhost" +
                    ":" +
                    "5432" +
                    "/" +
                    "postgres" +
                    "?user=" +
                    "postgres" +
                    "&password=" +
                    "root";
            return DriverManager.getConnection(url);
        } catch (SQLException e) {
            System.err.println("Cannot connect to the database: " + e.getMessage());
            return null;
        }
    }

    private static void connectToDb() {
        try {
            Class.forName("org.postgresql.Driver").getDeclaredConstructor().newInstance();
        } catch (InstantiationException|IllegalAccessException|ClassNotFoundException e){
            System.err.println("Can’t load driver. Verify CLASSPATH");
            System.err.println(e.getMessage());
        } catch (InvocationTargetException | NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }
}