package org.jevis.sqldatasource;

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.jevis.api.JEVisAttribute;
import org.jevis.api.JEVisClass;
import org.jevis.api.JEVisException;
import org.jevis.api.JEVisObject;
import org.jevis.api.JEVisType;
import org.jevis.commons.DatabaseHelper;
import org.jevis.commons.driver.DataSourceHelper;
import org.jevis.commons.driver.Importer;
import org.jevis.commons.driver.ImporterFactory;
import org.jevis.commons.driver.DataCollectorTypes;
import org.jevis.commons.driver.Parser;
import org.jevis.commons.driver.Result;
import org.jevis.commons.driver.DataSource;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;


interface MSSQLChannelDirectory extends DataCollectorTypes.ChannelDirectory {

    public final static String NAME = "MSSQL Channel Directory";
}

interface MSSQL extends DataCollectorTypes.DataSource.DataServer {
    //public final static String NAME = "Data Server";
    //public final static String CONNECTION_TIMEOUT = "Connection Timeout";
    //public final static String READ_TIMEOUT = "Read Timeout";
    //public final static String HOST = "Host";
    //public final static String PORT = "Port";

    public final static String NAME = "MSSQL Server";
    public final static String SCHEMA = "Schema";
    public final static String USER = "User";
    public final static String PASSWORD = "Password";
}
interface MSSQLChannel extends DataCollectorTypes.Channel {

    public final static String NAME = "MSSQL Channel";
    public final static String TABLE = "Table";
    public final static String COL_ID = "Column ID";
    public final static String COL_TS = "Column Timestamp";
    public final static String COL_TS_FORMAT = "Timestamp Format";
    public final static String COL_VALUE = "Column Value";
}

interface MSSQLDataPointDirectory extends DataCollectorTypes.DataPointDirectory {

    public final static String NAME = "MSSQL Data Point Directory";
}

interface MSSQLDataPoint extends DataCollectorTypes.DataPoint {

    public final static String NAME = "MSSQL Data Point";
    public final static String ID = "ID";
    public final static String TARGET = "Target";
}

/**
 * The structure for a single data point must be at least:
 * SQL Server
 * - SQL Channel Directory
 *   - Data Point Directory (Optional)
 *     - Data Point
 */


/**
 *
 * @author bf
 */
public class MSSQLDataSource implements DataSource {

    private Long _id;
    private String _name;
    private String _host;
    private Integer _port;
    private String _schema;
    private Integer _connectionTimeout;
    private Integer _readTimeout;
    private String _dbUser;
    private String _dbPW;
    private Boolean _ssl = false;
    private String _timezone;
    private Boolean _enabled;

    private Importer _importer;
    private List<JEVisObject> _channels;
    private List<Result> _result;

    private JEVisObject _dataSource;
    private Connection _con;

    @Override
    public void parse(List<InputStream> input) {}

    @Override
    public void run() {
        try {
            //String url ="jdbc:sqlserver://MYPC\\SQLEXPRESS;databaseName=MYDB;integratedSecurity=true";
            String url = "jdbc:sqlserver://" + _host + ":" + _port + "/" + _schema + "?";
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            _con = DriverManager.getConnection(url, _dbUser, _dbPW);
        } catch (ClassNotFoundException | SQLException ex) {
            java.util.logging.Logger.getLogger(MSSQLDataSource.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        }
        for (JEVisObject channel : _channels) {

            try {
                _result = new ArrayList<Result>();
                
                // MAYBE: get and initialize parser or write it in this file

                this.sendSampleRequest(channel); // also does the parsing

                if (!_result.isEmpty()) {

                    this.importResult();

                    DataSourceHelper.setLastReadout(channel, _importer.getLatestDatapoint());
                }
            } catch (Exception ex) {
                java.util.logging.Logger.getLogger(MSSQLDataSource.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
            }
        }
    }

    @Override
    public void importResult() {
        _importer.importResult(_result);
    }

    @Override
    public void initialize(JEVisObject mssqlObject) {
        _dataSource = mssqlObject;
        initializeAttributes(mssqlObject);
        initializeChannelObjects(mssqlObject);

        _importer = ImporterFactory.getImporter(_dataSource);
        if (_importer != null) {
            _importer.initialize(_dataSource);
        }
    }

    /**
     * 
     *
     * @param channel
     * @return
     */
    @Override
    public List<InputStream> sendSampleRequest(JEVisObject channel) {
        try {
            JEVisClass channelClass = channel.getJEVisClass();
            JEVisType tableType = channelClass.getType(MSSQLChannel.TABLE);
            JEVisType col_idType = channelClass.getType(MSSQLChannel.COL_ID);
            JEVisType col_tsType = channelClass.getType(MSSQLChannel.COL_TS);
            JEVisType col_tsFormatType = channelClass.getType(MSSQLChannel.COL_TS_FORMAT);
            JEVisType valueType = channelClass.getType(MSSQLChannel.COL_VALUE);
            String table = DatabaseHelper.getObjectAsString(channel, tableType);
            String col_id = DatabaseHelper.getObjectAsString(channel, col_idType);
            String col_ts = DatabaseHelper.getObjectAsString(channel, col_tsType);
            String col_ts_format = DatabaseHelper.getObjectAsString(channel, col_tsFormatType);
            String col_value = DatabaseHelper.getObjectAsString(channel, valueType);
            JEVisType readoutType = channelClass.getType(MSSQLChannel.LAST_READOUT);
            // TODO: this pattern should be in JECommons
            DateTime lastReadout = DatabaseHelper.getObjectAsDate(channel, readoutType, DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss"));
            String sql_lastReadout;
            DateTimeFormatter dbDateTimeFormatter = DateTimeFormat.forPattern(col_ts_format);
            sql_lastReadout = lastReadout.toString(DateTimeFormat.forPattern(col_ts_format));
            
            String sql_query = String.format("select %s, %s, %s", col_id, col_ts, col_value);
            sql_query += " from " + table;
            sql_query += " where " + col_ts + " > " + sql_lastReadout;
            sql_query += " and " + col_id + " =?";
            sql_query += ";";
            PreparedStatement ps = _con.prepareStatement(sql_query);
            
            List<JEVisObject> _dataPoints;
            try {
                // Get all datapoints under the current channel
                JEVisClass dpClass = channel.getDataSource().getJEVisClass(MSSQLDataPoint.NAME);
                _dataPoints = channel.getChildren(dpClass, true);
                
            } catch (JEVisException ex) {
                java.util.logging.Logger.getLogger(MSSQLDataSource.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
                return null;
            }
            // Create query for each datapoint
            for (JEVisObject dp : _dataPoints) {
                JEVisClass dpClass = dp.getJEVisClass();

                JEVisType idType = dpClass.getType(MSSQLDataPoint.ID);
                JEVisType targetType = dpClass.getType(DataCollectorTypes.DataPoint.CSVDataPoint.TARGET);
                
                String id = DatabaseHelper.getObjectAsString(dp, idType);
                Long target = DatabaseHelper.getObjectAsLong(dp, targetType);
                
                // Querry for ID given by the datapoint
                ps.setString(1, id);
                ResultSet rs = ps.executeQuery();
                
                try {
                    // Parse the results
                    while (rs.next()) {
                        String ts_str = rs.getString(col_ts);
                        String val_str = rs.getString(col_value);

                        // Parse value and timestamp
                        double value = Double.parseDouble(val_str);
                        DateTime dateTime = dbDateTimeFormatter.parseDateTime(ts_str);
                        
                        // add to results
                        _result.add(new Result(target, value, dateTime));
                    }
                } catch (NumberFormatException nfe) {
                    java.util.logging.Logger.getLogger(MSSQLDataSource.class.getName()).log(java.util.logging.Level.SEVERE, null, nfe);
                } catch (SQLException ex) {
                    java.util.logging.Logger.getLogger(MSSQLDataSource.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
                }
            }
            
        } catch (JEVisException ex) {
            java.util.logging.Logger.getLogger(MSSQLDataSource.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        } catch (SQLException ex) {
            java.util.logging.Logger.getLogger(MSSQLDataSource.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        }
        return null;
    }

    private void initializeAttributes(JEVisObject mssqlObject) {
        try {
            JEVisClass mssqlType = mssqlObject.getDataSource().getJEVisClass(MSSQL.NAME);
            JEVisType host = mssqlType.getType(MSSQL.HOST);
            JEVisType port = mssqlType.getType(MSSQL.PORT);
            JEVisType schema = mssqlType.getType(MSSQL.SCHEMA);
            JEVisType user = mssqlType.getType(MSSQL.USER);
            JEVisType password = mssqlType.getType(MSSQL.PASSWORD);
            JEVisType connectionTimeout = mssqlType.getType(MSSQL.CONNECTION_TIMEOUT);
            JEVisType readTimeout = mssqlType.getType(MSSQL.READ_TIMEOUT);
            JEVisType timezoneType = mssqlType.getType(MSSQL.TIMEZONE);
            JEVisType enableType = mssqlType.getType(MSSQL.ENABLE);

            _id = mssqlObject.getID();
            _name = mssqlObject.getName();
            _host = DatabaseHelper.getObjectAsString(mssqlObject, host);
            _port = DatabaseHelper.getObjectAsInteger(mssqlObject, port);
            _schema = DatabaseHelper.getObjectAsString(mssqlObject, schema);
            JEVisAttribute userAttr = mssqlObject.getAttribute(user);
            if (!userAttr.hasSample()) {
                _dbUser = "";
            } else {
                _dbUser = (String) userAttr.getLatestSample().getValue();
            }
            JEVisAttribute passAttr = mssqlObject.getAttribute(password);
            if (!passAttr.hasSample()) {
                _dbPW = "";
            } else {
                _dbPW = (String) passAttr.getLatestSample().getValue();
            }
            
            _connectionTimeout = DatabaseHelper.getObjectAsInteger(mssqlObject, connectionTimeout);
            _readTimeout = DatabaseHelper.getObjectAsInteger(mssqlObject, readTimeout);
            _timezone = DatabaseHelper.getObjectAsString(mssqlObject, timezoneType);
            _enabled = DatabaseHelper.getObjectAsBoolean(mssqlObject, enableType);
        } catch (JEVisException ex) {
            Logger.getLogger(MSSQLDataSource.class.getName()).log(Level.ERROR, null, ex);
        }
    }

    private void initializeChannelObjects(JEVisObject mssqlObject) {
        try {
            JEVisClass channelDirClass = mssqlObject.getDataSource().getJEVisClass(MSSQLChannelDirectory.NAME);
            JEVisObject channelDir = mssqlObject.getChildren(channelDirClass, false).get(0);
            JEVisClass channelClass = mssqlObject.getDataSource().getJEVisClass(MSSQLChannel.NAME);
            _channels = channelDir.getChildren(channelClass, false);
        } catch (JEVisException ex) {
            java.util.logging.Logger.getLogger(MSSQLDataSource.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        }
    }

}
