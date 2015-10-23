# MSSQL-Driver
Genric MSSQL Data Source Driver for the JEDataCollector 

## Dependencies
- [SQLDriverAbstract](https://github.com/AIT-JEVis/SQL-Driver-abstract)
- [jdts](http://jtds.sourceforge.net/)

## Installation
- Have a up and running JEVis3 instance
- Build dependency SQL-Driver-abstract
```
git clone https://github.com/AIT-JEVis/SQL-Driver-abstract
cd SQL-Driver-abstract
mvn install
cd ..
```

- Clone this repository and build the driver-jar file
```
git clone https://github.com/AIT-JEVis/MSSQL-Driver.git
cd MSSQL-Driver
mvn package
```

- Upload the driver to JEVis using JSON2JEVisStructureCreator (assuming [JSON2JEVisStructureCreator](https://github.com/AIT-JEVis/JSON2JEVisStructureCreator) is already built)
```
java -jar ../JSON2JEVisStructureCreator/target/JSON2JEVisStructureCreator-with-dependencies-jar MSSQLDriverObjects.json
```

## Configuration
TODO

### Windows authentication
If the Attribute 'Domain' is set, then MSSQL-Driver tries to connect using Windows Authentication. You can either provide the username and password to log in or use the native Windows Authentication. For this install `ntlmauth.dll` in your `PATH` on the windows machine the DataCollector needs to authenticate using Windows Signle Sign On feature.

