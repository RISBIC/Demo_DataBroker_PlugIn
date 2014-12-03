/*
 * Copyright (c) 2014, Arjuna Technologies Limited, Newcastle-upon-Tyne, England. All rights reserved.
 */

package org.risbic.dbplugins.demo.spreadsheet2jdbc;

import java.io.File;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.xssf.eventusermodel.XSSFReader;
import org.apache.poi.xssf.model.SharedStringsTable;
import org.apache.poi.xssf.usermodel.XSSFRichTextString;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;
import org.xml.sax.helpers.XMLReaderFactory;
import com.arjuna.databroker.data.DataConsumer;
import com.arjuna.databroker.data.DataFlow;
import com.arjuna.databroker.data.DataProcessor;
import com.arjuna.databroker.data.DataProvider;
import com.arjuna.databroker.data.jee.annotation.DataConsumerInjection;
import com.arjuna.databroker.data.jee.annotation.DataProviderInjection;
import com.arjuna.databroker.data.jee.annotation.PostConfig;
import com.arjuna.databroker.data.jee.annotation.PostCreated;
import com.arjuna.databroker.data.jee.annotation.PreConfig;
import com.arjuna.databroker.data.jee.annotation.PreDelete;

public class Spreadsheet2JDBCProcessor implements DataProcessor
{
    private static final Logger logger = Logger.getLogger(Spreadsheet2JDBCProcessor.class.getName());

    public static final String DATABASE_CONNECTIONURL_PROPNAME = "Database Connection URL";
    public static final String DATABASE_USERNAME_PROPNAME      = "Database Username";
    public static final String DATABASE_PASSWORD_PROPNAME      = "Database Password";

    public Spreadsheet2JDBCProcessor(String name, Map<String, String> properties)
    {
        logger.log(Level.FINE, "XSSFDataProcessor: " + name + ", " + properties);

        _name       = name;
        _properties = properties;

        _connection = null;
    }

    @Override
    public String getName()
    {
        return _name;
    }

    @Override
    public void setName(String name)
    {
        _name = name;
    }

    @Override
    public Map<String, String> getProperties()
    {
        return Collections.unmodifiableMap(_properties);
    }

    @Override
    public void setProperties(Map<String, String> properties)
    {
        _properties = properties;
    }

    @Override
    public DataFlow getDataFlow()
    {
        return _dataFlow;
    }

    @Override
    public void setDataFlow(DataFlow dataFlow)
    {
        _dataFlow = dataFlow;
    }

    @PostCreated
    @PostConfig
    public void startup()
    {
        try
        {
            String connectionURL = _properties.get(DATABASE_CONNECTIONURL_PROPNAME);
            String username      = _properties.get(DATABASE_USERNAME_PROPNAME);
            String password      = _properties.get(DATABASE_PASSWORD_PROPNAME);

            _connection = DriverManager.getConnection(connectionURL, username, password);
        }
        catch (Throwable throwable)
        {
            logger.log(Level.WARNING, "", throwable);
        }
    }

    @PreConfig
    @PreDelete
    public void shutdown()
    {
        try
        {
            if (_connection != null)
                _connection.close();
        }
        catch (Throwable throwable)
        {
            logger.log(Level.WARNING, "", throwable);
        }
    }

    private class WorkbookHandler extends DefaultHandler
    {
        private static final String SPREADSHEETML_NAMESPACE = "http://schemas.openxmlformats.org/spreadsheetml/2006/main";
        private static final String RELATIONSHIPS_NAMESPACE = "http://schemas.openxmlformats.org/officeDocument/2006/relationships";
        private static final String NONE_NAMESPACE          = "";
        private static final String SHEET_TAGNAME           = "sheet";
        private static final String NAME_ATTRNAME           = "name";
        private static final String ID_ATTRNAME             = "id";

        public WorkbookHandler(Map<String, String> refIdMap)
        {
             _refIdMap = refIdMap;
        }

        @Override
        public void startElement(String uri, String localName, String qName, Attributes attributes)
            throws SAXException
        {
            if ((localName != null) && localName.equals(SHEET_TAGNAME) && (uri != null) && uri.equals(SPREADSHEETML_NAMESPACE))
            {
                String name = attributes.getValue(NONE_NAMESPACE, NAME_ATTRNAME);
                String id    = attributes.getValue(RELATIONSHIPS_NAMESPACE, ID_ATTRNAME);

                if (name != null)
                    _refIdMap.put(name, id);
            }
        }

        private Map<String, String> _refIdMap;
    }

    private static final String[] KEYS = { "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M",
    	                                   "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z",
                                           "AA", "AB", "AC", "AD", "AE", "AF", "AG", "AH", "AI", "AJ", "AK", "AL", "AM",
                                           "AN", "AO", "AP", "AQ", "AR", "AS", "AT", "AU" };

    private class SheetHandler extends DefaultHandler
    {
        private static final String SPREADSHEETML_NAMESPACE = "http://schemas.openxmlformats.org/spreadsheetml/2006/main";
        private static final String NONE_NAMESPACE          = "";
        private static final String ROW_TAGNAME             = "row";
        private static final String CELL_TAGNAME            = "c";
        private static final String VALUE_TAGNAME           = "v";
        private static final String SHEETDATA_TAGNAME       = "sheetData";
        private static final String REF_ATTRNAME            = "r";
        private static final String TYPE_ATTRNAME           = "t";
        private static final String STYLE_ATTRNAME          = "s";

        public SheetHandler(String tableName, SharedStringsTable sharedStringsTable)
        {
        	_tableName          = tableName;
            _sharedStringsTable = sharedStringsTable;
            _cellName           = null;
            _cellType           = null;
            _cellStyle          = null;
            _value              = new StringBuffer();
            _rowMap             = new LinkedHashMap<String, String>();
            _rowCount           = 0;
        }

        @Override
        public void startElement(String uri, String localName, String qName, Attributes attributes)
            throws SAXException
        {
            if ((localName != null) && localName.equals(CELL_TAGNAME) && (uri != null) && uri.equals(SPREADSHEETML_NAMESPACE))
            {
                _cellName  = attributes.getValue(NONE_NAMESPACE, REF_ATTRNAME);
                _cellType  = attributes.getValue(NONE_NAMESPACE, TYPE_ATTRNAME);
                _cellStyle = attributes.getValue(NONE_NAMESPACE, STYLE_ATTRNAME);
            }
            else if ((localName != null) && localName.equals(VALUE_TAGNAME) && (uri != null) && uri.equals(SPREADSHEETML_NAMESPACE))
                _value.setLength(0);
        }

        @Override
        public void endElement(String uri, String localName, String qName)
            throws SAXException
        {
            if ((localName != null) && localName.equals(VALUE_TAGNAME) && (uri != null) && uri.equals(SPREADSHEETML_NAMESPACE))
            {
            	if (_cellType.equals("n"))
                    _rowMap.put(removeRowNumber(_cellName), _value.toString());
            	else if (_cellType.equals("s"))
            	{
                    String sharedStringsTableIndex = _value.toString();
                    try
                    {
                        int idx = Integer.parseInt(sharedStringsTableIndex);
                        XSSFRichTextString rtss = new XSSFRichTextString(_sharedStringsTable.getEntryAt(idx));
                        _rowMap.put(removeRowNumber(_cellName), rtss.toString());
                    }
                    catch (NumberFormatException numberFormatException)
                    {
                        logger.log(Level.WARNING, "Failed to parse 'Shared Strings Table' index '" + sharedStringsTableIndex + "'", numberFormatException);
                    }
                }
            	else
            		logger.log(Level.WARNING, "Unsupported cell type '" + _cellType + "'");

            	_value.setLength(0);
            }
            else if ((localName != null) && localName.equals(ROW_TAGNAME) && (uri != null) && uri.equals(SPREADSHEETML_NAMESPACE))
            {
                String sql = rowMap2SQL(_rowMap);
                if (logger.isLoggable(Level.FINE))
                    logger.log(Level.FINE, "SQL: [" + sql + "]");

                Statement statement = null;
                try
                {
                    statement = _connection.createStatement();
                    statement.executeUpdate(sql);
                    statement.close();

                    _rowCount++;
                }
                catch (Throwable throwable)
                {
                	logger.log(Level.WARNING, "Problem adding data: \'" + sql + "\'", throwable);
                }

                _rowMap.clear();
            }
            else if ((localName != null) && localName.equals(SHEETDATA_TAGNAME) && (uri != null) && uri.equals(SPREADSHEETML_NAMESPACE))
                _dataProvider.produce(_tableName);
        }

        @Override
        public void characters(char[] characters, int start, int length)
            throws SAXException
        {
            _value.append(characters, start, length);
        }

        private String rowMap2SQL(Map<String, String> rowMap)
        {
            StringBuffer sql = new StringBuffer();

            sql.append("INSERT INTO ");
            sql.append(_tableName);
            sql.append(" VALUES (");
            boolean first = true;
            for (String key: KEYS)
            {
                if (! first)
                    sql.append(',');
                else
                    first = false;

                String value = rowMap.get(key);

                sql.append('\'');
                if (value != null)
                    sql.append(sqlEscape(value));
                sql.append('\'');
            }
            sql.append(",");
            sql.append(Long.toString(_rowCount));
            sql.append(");");

            return sql.toString();
        }

        private String removeRowNumber(String cellName)
        {
            int index = 0;
            while ((index < cellName.length()) && Character.isAlphabetic(cellName.charAt(index)))
                index++;

            return cellName.substring(0, index);
        }

        private String sqlEscape(String sql)
        {
//            return sql.replaceAll("\\\\", "\\\\\\\\").replaceAll("'", "\\\\'");
            return sql.replaceAll("\\\\", "\\\\\\\\").replaceAll("'", "");
        }

        private String              _tableName;
        private SharedStringsTable  _sharedStringsTable;
        private String              _cellName;
        private String              _cellType;
        private String              _cellStyle;
        private StringBuffer        _value;
        private Map<String, String> _rowMap;
        private long                _rowCount;
    }

    public void consume(File data)
    {
        try
        {
            String tableName = createTable();

            Map<String, String> refIdMap = new HashMap<String, String>();

            OPCPackage         opcPackage         = OPCPackage.open(data);
            XSSFReader         xssfReader         = new XSSFReader(opcPackage);
            SharedStringsTable sharedStringsTable = xssfReader.getSharedStringsTable();

            XMLReader      workbookParser  = XMLReaderFactory.createXMLReader("org.apache.xerces.parsers.SAXParser");
            ContentHandler workbookHandler = new WorkbookHandler(refIdMap);
            workbookParser.setContentHandler(workbookHandler);

            InputStream workbookInputStream = xssfReader.getWorkbookData();
            InputSource workbookSource        = new InputSource(workbookInputStream);
            workbookParser.parse(workbookSource);
            workbookInputStream.close();

            XMLReader      sheetParser  = XMLReaderFactory.createXMLReader("org.apache.xerces.parsers.SAXParser");
            ContentHandler sheetHandler = new SheetHandler(tableName, sharedStringsTable);
            sheetParser.setContentHandler(sheetHandler);

            Iterator<InputStream> sheetInputStreamIterator = xssfReader.getSheetsData();
            while (sheetInputStreamIterator.hasNext())
            {
                InputStream sheetInputStream = sheetInputStreamIterator.next();
                InputSource sheetSource = new InputSource(sheetInputStream);
                sheetParser.parse(sheetSource);
                sheetInputStream.close();
            }
        }
        catch (Throwable throwable)
        {
            logger.log(Level.WARNING, "Problem processing XSSF file \"" + data.getAbsolutePath() + "\"", throwable);
        }
    }

    private String createTable()
    {
        String tableName = "table_" + UUID.randomUUID().toString().replace('-', '_');

        try
        {
            StringBuffer createCommentBuffer = new StringBuffer();

            createCommentBuffer.append("CREATE TABLE ");
            createCommentBuffer.append(tableName);
            createCommentBuffer.append("\n (\n");
            createCommentBuffer.append("     companyname TEXT,\n");
            createCommentBuffer.append("     companynumber TEXT,\n");
            createCommentBuffer.append("     regaddresscareof TEXT,\n");
            createCommentBuffer.append("     regaddresspobox TEXT,\n");
            createCommentBuffer.append("     regaddressaddressline1 TEXT,\n");
            createCommentBuffer.append("     regaddressaddressline2 TEXT,\n");
            createCommentBuffer.append("     regaddressposttown TEXT,\n");
            createCommentBuffer.append("     regaddresscounty TEXT,\n");
            createCommentBuffer.append("     regaddresscountry TEXT,\n");
            createCommentBuffer.append("     regaddresspostcode TEXT,\n");
            createCommentBuffer.append("     companycategory TEXT,\n");
            createCommentBuffer.append("     companystatus TEXT,\n");
            createCommentBuffer.append("     countryoforigin TEXT,\n");
            createCommentBuffer.append("     dissolutiondate TEXT,\n");
            createCommentBuffer.append("     incorporationdate TEXT,\n");
            createCommentBuffer.append("     accountsaccountrefday TEXT,\n");
            createCommentBuffer.append("     accountsaccountrefmonth TEXT,\n");
            createCommentBuffer.append("     accountsnextduedate TEXT,\n");
            createCommentBuffer.append("     accountslastmadeupdate TEXT,\n");
            createCommentBuffer.append("     accountsaccountcategory TEXT,\n");
            createCommentBuffer.append("     returnsnextduedate TEXT,\n");
            createCommentBuffer.append("     returnslastmadeupdate TEXT,\n");
            createCommentBuffer.append("     mortgagesnummortcharges TEXT,\n");
            createCommentBuffer.append("     mortgagesnummortoutstanding TEXT,\n");
            createCommentBuffer.append("     mortgagesnummortpartsatisfied TEXT,\n");
            createCommentBuffer.append("     mortgagesnummortsatisfied TEXT,\n");
            createCommentBuffer.append("     siccodesictext_1 TEXT,\n");
            createCommentBuffer.append("     siccodesictext_2 TEXT,\n");
            createCommentBuffer.append("     siccodesictext_3 TEXT,\n");
            createCommentBuffer.append("     siccodesictext_4 TEXT,\n");
            createCommentBuffer.append("     limitedpartnershipsnumgenpartners TEXT,\n");
            createCommentBuffer.append("     limitedpartnershipsnumlimpartners TEXT,\n");
            createCommentBuffer.append("     uri TEXT,\n");
            createCommentBuffer.append("     previousname_1condate TEXT,\n");
            createCommentBuffer.append("     previousname_1companyname TEXT,\n");
            createCommentBuffer.append("     previousname_2condate TEXT,\n");
            createCommentBuffer.append("     previousname_2companyname TEXT,\n");
            createCommentBuffer.append("     previousname_3condate TEXT,\n");
            createCommentBuffer.append("     previousname_3companyname TEXT,\n");
            createCommentBuffer.append("     previousname_4condate TEXT,\n");
            createCommentBuffer.append("     previousname_4companyname TEXT,\n");
            createCommentBuffer.append("     previousname_5condate TEXT,\n");
            createCommentBuffer.append("     previousname_5companyname TEXT,\n");
            createCommentBuffer.append("     previousname_6condate TEXT,\n");
            createCommentBuffer.append("     previousname_6companyname TEXT,\n");
            createCommentBuffer.append("     previousname_7condate TEXT,\n");
            createCommentBuffer.append("     previousname_7companyname TEXT,\n");
            createCommentBuffer.append("     previousname_8condate TEXT,\n");
            createCommentBuffer.append("     previousname_8companyname TEXT,\n");
            createCommentBuffer.append("     previousname_9condate TEXT,\n");
            createCommentBuffer.append("     previousname_9companyname TEXT,\n");
            createCommentBuffer.append("     previousname_10condate TEXT,\n");
            createCommentBuffer.append("     previousname_10companyname TEXT,\n");
            createCommentBuffer.append("     cleancompanyname TEXT,\n");
            createCommentBuffer.append("     id BIGSERIAL NOT NULL,\n");
            createCommentBuffer.append("     CONSTRAINT ");
            createCommentBuffer.append(tableName);
            createCommentBuffer.append("_pkey PRIMARY KEY (id)\n");
            createCommentBuffer.append(") WITH (OIDS=FALSE);");

            Statement statement = _connection.createStatement();

            statement.executeUpdate(createCommentBuffer.toString());

            statement.close();

            return tableName;
        }
        catch (Throwable throwable)
        {
            logger.log(Level.WARNING, "Problem creating table: \'" + tableName + "\'", throwable);
            return null;
        }
    }

    @Override
    public Collection<Class<?>> getDataConsumerDataClasses()
    {
        Set<Class<?>> dataConsumerDataClasses = new HashSet<Class<?>>();

        dataConsumerDataClasses.add(File.class);

        return dataConsumerDataClasses;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> DataConsumer<T> getDataConsumer(Class<T> dataClass)
    {
        if (File.class.isAssignableFrom(dataClass))
            return (DataConsumer<T>) _dataConsumer;
        else
            return null;
    }

    @Override
    public Collection<Class<?>> getDataProviderDataClasses()
    {
        Set<Class<?>> dataProviderDataClasses = new HashSet<Class<?>>();

        dataProviderDataClasses.add(String.class);

        return dataProviderDataClasses;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> DataProvider<T> getDataProvider(Class<T> dataClass)
    {
        if (String.class.isAssignableFrom(dataClass))
            return (DataProvider<T>) _dataProvider;
        else
            return null;
    }

    private Connection _connection;
    
    private String               _name;
    private Map<String, String>  _properties;
    private DataFlow             _dataFlow;
    @DataConsumerInjection(methodName="consume")
    private DataConsumer<File>   _dataConsumer;
    @DataProviderInjection
    private DataProvider<String> _dataProvider;
}