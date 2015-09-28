package org.jevis.sqldatasource;

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
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


interface SQLChannelDirectory extends DataCollectorTypes.ChannelDirectory {

    public final static String NAME = "SQL Channel Directory";
}

interface SQL extends DataCollectorTypes.DataSource.DataServer {
    
    public final static String NAME = "SQL Server";
    public final static String PASSWORD = "Password";
    public final static String SSL = "SSL";
    public final static String USER = "User";
}
interface SQLChannel extends DataCollectorTypes.Channel {

    public final static String NAME = "SQL Channel";
    public final static String PATH = "Path";
}



/**
 *
 * @author bf
 */
public class MSSQLDataSource implements DataSource {

    private Long _id;
    private String _name;
    private String _serverURL;
    private Integer _port;
    private Integer _connectionTimeout;
    private Integer _readTimeout;
    private String _userName;
    private String _password;
    private Boolean _ssl = false;
    private String _timezone;
    private Boolean _enabled;

    private Parser _parser;
    private Importer _importer;
    private List<JEVisObject> _channels;
    private List<Result> _result;

    private JEVisObject _dataSource;

    @Override
    public void parse(List<InputStream> input) {
        _parser.parse(input);
        _result = _parser.getResult();
    }

    @Override
    public void run() {
        try {
            String url ="jdbc:sqlserver://MYPC\\SQLEXPRESS;databaseName=MYDB;integratedSecurity=true";
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            Connection conn = DriverManager.getConnection(url, _userName, _password);
        } catch (ClassNotFoundException | SQLException ex) {
            java.util.logging.Logger.getLogger(MSSQLDataSource.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        }
        for (JEVisObject channel : _channels) {

            try {
                _result = new ArrayList<Result>();
                
                // MAYBE: get and initialize parser or write it in this file

                List<InputStream> input = this.sendSampleRequest(channel);

                this.parse(input);

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
     * TODO: komplett überarbeiten!!!!!
     *
     * @param channel
     * @return
     */
    @Override
    public List<InputStream> sendSampleRequest(JEVisObject channel) {
        List<InputStream> answer = new ArrayList<InputStream>();
        try {
            JEVisClass channelClass = channel.getJEVisClass();
            JEVisType pathType = channelClass.getType(SQLChannel.PATH);
            String path = DatabaseHelper.getObjectAsString(channel, pathType);
            JEVisType readoutType = channelClass.getType(SQLChannel.LAST_READOUT);
            // TODO: this pattern should be in JECommons
            DateTime lastReadout = DatabaseHelper.getObjectAsDate(channel, readoutType, DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss"));
            
            

        } catch (JEVisException ex) {
            java.util.logging.Logger.getLogger(MSSQLDataSource.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        } catch (Exception ex) {
            java.util.logging.Logger.getLogger(MSSQLDataSource.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        }
        return answer;
    }

    private void initializeAttributes(JEVisObject httpObject) {
        try {
            JEVisClass httpType = httpObject.getDataSource().getJEVisClass(SQL.NAME);
            JEVisType server = httpType.getType(SQL.HOST);
            JEVisType port = httpType.getType(SQL.PORT);
            JEVisType sslType = httpType.getType(SQL.SSL);
            JEVisType connectionTimeout = httpType.getType(SQL.CONNECTION_TIMEOUT);
            JEVisType readTimeout = httpType.getType(SQL.READ_TIMEOUT);
            JEVisType user = httpType.getType(SQL.USER);
            JEVisType password = httpType.getType(SQL.PASSWORD);
            JEVisType timezoneType = httpType.getType(SQL.TIMEZONE);
            JEVisType enableType = httpType.getType(SQL.ENABLE);

            _id = httpObject.getID();
            _name = httpObject.getName();
            _serverURL = DatabaseHelper.getObjectAsString(httpObject, server);
            _port = DatabaseHelper.getObjectAsInteger(httpObject, port);
            _connectionTimeout = DatabaseHelper.getObjectAsInteger(httpObject, connectionTimeout);
            _readTimeout = DatabaseHelper.getObjectAsInteger(httpObject, readTimeout);
            _ssl = DatabaseHelper.getObjectAsBoolean(httpObject, sslType);
            JEVisAttribute userAttr = httpObject.getAttribute(user);
            if (!userAttr.hasSample()) {
                _userName = "";
            } else {
                _userName = (String) userAttr.getLatestSample().getValue();
            }
            JEVisAttribute passAttr = httpObject.getAttribute(password);
            if (!passAttr.hasSample()) {
                _password = "";
            } else {
                _password = (String) passAttr.getLatestSample().getValue();
            }
            _timezone = DatabaseHelper.getObjectAsString(httpObject, timezoneType);
            _enabled = DatabaseHelper.getObjectAsBoolean(httpObject, enableType);
        } catch (JEVisException ex) {
            Logger.getLogger(MSSQLDataSource.class.getName()).log(Level.ERROR, null, ex);
        }
    }

    private void initializeChannelObjects(JEVisObject httpObject) {
        try {
            JEVisClass channelDirClass = httpObject.getDataSource().getJEVisClass(SQLChannelDirectory.NAME);
            JEVisObject channelDir = httpObject.getChildren(channelDirClass, false).get(0);
            JEVisClass channelClass = httpObject.getDataSource().getJEVisClass(SQLChannel.NAME);
            _channels = channelDir.getChildren(channelClass, false);
        } catch (JEVisException ex) {
            java.util.logging.Logger.getLogger(MSSQLDataSource.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        }
    }

}
