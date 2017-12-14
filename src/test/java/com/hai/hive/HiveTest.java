package com.hai.hive;

import com.hai.hive.model.Employee;
import org.apache.commons.collections.map.HashedMap;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;
import java.util.Map;

/**
 * Created by as on 2017/3/29.
 */
public class HiveTest {
    private static final String ConnectionDriverName = "org.apache.hive.jdbc.HiveDriver";
    private static final String ConnectionURL = "jdbc:hive2://s1:10000/hive1";
    private static final String ConnectionUserName = "";
    private static final String ConnectionPassword = "";
//    private static final String ConnectionUserName = "hive";
//    private static final String ConnectionPassword = "hivehive";

    private Connection connection;
    private Statement statement;
    private PreparedStatement preparedStatement;
    private ResultSet resultSet;

    @Before
    public void before() throws ClassNotFoundException, SQLException {
        System.out.println("before...");
        Class.forName(ConnectionDriverName);
        connection = DriverManager.getConnection(ConnectionURL, ConnectionUserName, ConnectionPassword);
    }

    @Test
    public void list() throws SQLException {
        preparedStatement = connection.prepareStatement("SELECT * FROM hive1.employee");
        resultSet = preparedStatement.executeQuery();
        printResutlSet();
    }

    private Map<String, Object> printResutlSet() throws SQLException {
        Map<String, Object> result = new HashedMap();
        if (null != resultSet) {
            System.out.println("printResutlSet...");
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            int columns = resultSetMetaData.getColumnCount();

            for (int index = 1; index <= columns; index++) {
                String key = resultSetMetaData.getColumnName(index);
                System.out.print(key + "\t");
            }
            System.out.println();

            Employee employee = null;
            Employee.Partition partition = null;
            while (resultSet.next()) {
//                employee=new Employee();
//                partition=new Employee.Partition();
//                String key = resultSetMetaData.getColumnName(index);
//                Object val = resultSet.getObject("employee.id");
//                System.out.println(resultSet.getObject("id"));
//                System.out.println(resultSet.getObject("employee.name"));
//                System.out.println(resultSet.getObject("employee.salary"));
//                System.out.println(resultSet.getObject("employee.destination"));
//                System.out.println(resultSet.getObject("employee.province"));
//                System.out.println(resultSet.getObject("employee.city"));
//                System.out.print(key + " : " + val + "\t");
//                index++;

                for (int index = 1; index <= columns; index++) {
                    String key = resultSetMetaData.getColumnName(index);
                    Object val = resultSet.getObject(index);
                    result.put(key, val);
                    System.out.print(val + "\t");
                }
                System.out.println();

            }
        } else {
            System.out.println("no results!");
        }
        return result;
    }

    @After
    public void after() throws SQLException {
        System.out.println("after...");
        if (null != preparedStatement) {
            preparedStatement.clearParameters();
        }
        if (null != statement) {
            statement.close();
        }
        if (null != connection) {
            connection.close();
        }
    }
}
