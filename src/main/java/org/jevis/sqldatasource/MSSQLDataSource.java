/**
 * Copyright (C) 2015 NeroBurner
 *
 * This program is free software: you can redistribute it and/or modify it under
 * the terms of the GNU General Public License as published by the Free Software
 * Foundation in version 3.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
 * A PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program. If not, see <http://www.gnu.org/licenses/>.
 *
 * This driver is part of the OpenJEVis project, further project information are
 * published at <http://www.OpenJEVis.org/>.
 */

package org.jevis.sqldatasource;

import java.sql.DriverManager;
import java.sql.SQLException;

import org.jevis.sqldatasource.SQLDriverAbstract;



/**
 * This is a driver to connect to a Microsoft SQL Database.
 * 
 * The structure in JEVis for a single data point must be at least:
 * SQL Server
 * - SQL Channel Directory
 *   - Data Point Directory (Optional)
 *     - Data Point
 * 
 * @author NeroBurner
 */
public class MSSQLDataSource extends SQLDriverAbstract {
    
    interface MSSQL extends SQLDriverAbstract.SQL {
        // from parent-class
        //public final static String NAME = "Data Server";
        //public final static String CONNECTION_TIMEOUT = "Connection Timeout";
        //public final static String READ_TIMEOUT = "Read Timeout";
        //public final static String HOST = "Host";
        //public final static String PORT = "Port";

        //public final static String SCHEMA = "Schema";
        //public final static String USER = "User";
        //public final static String PASSWORD = "Password";
        
        public final static String NAME = "MSSQL Server";
    }
    @Override
    public String loadJDBC(String host, int port, String schema, String dbUser, String dbPW) throws ClassNotFoundException, SQLException {
        //String url ="jdbc:sqlserver://MYPC\\SQLEXPRESS;databaseName=MYDB;integratedSecurity=true";
        String url = "jdbc:sqlserver://" + host + ":" + port + "/" + schema + "?";
        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        _con = DriverManager.getConnection(url, dbUser, dbPW);
        
        return url;
    }
    
    @Override
    protected String getClassName() {
        return MSSQL.NAME;
    }

}
