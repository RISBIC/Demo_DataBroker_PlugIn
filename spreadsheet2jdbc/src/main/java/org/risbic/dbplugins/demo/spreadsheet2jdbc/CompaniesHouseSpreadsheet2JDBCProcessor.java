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
import org.apache.poi.ss.usermodel.BuiltinFormats;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.xssf.eventusermodel.XSSFReader;
import org.apache.poi.xssf.model.SharedStringsTable;
import org.apache.poi.xssf.model.StylesTable;
import org.apache.poi.xssf.usermodel.XSSFCellStyle;
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

public class CompaniesHouseSpreadsheet2JDBCProcessor implements DataProcessor
{
    private static final Logger logger = Logger.getLogger(CompaniesHouseSpreadsheet2JDBCProcessor.class.getName());

    public static final String DATABASE_CONNECTIONURL_PROPNAME = "Database Connection URL";
    public static final String DATABASE_USERNAME_PROPNAME      = "Database Username";
    public static final String DATABASE_PASSWORD_PROPNAME      = "Database Password";

    public CompaniesHouseSpreadsheet2JDBCProcessor()
    {
        logger.log(Level.FINE, "CompaniesHouseSpreadsheet2JDBCProcessor);
    }

    public CompaniesHouseSpreadsheet2JDBCProcessor(String name, Map<String, String> properties)
    {
        logger.log(Level.FINE, "CompaniesHouseSpreadsheet2JDBCProcessor: " + name + ", " + properties);

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

        public SheetHandler(String tableName, SharedStringsTable sharedStringsTable, StylesTable stylesTable)
        {
            _tableName          = tableName;
            _sharedStringsTable = sharedStringsTable;
            _stylesTable        = stylesTable;
            _formatter          = new DataFormatter();
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
            try
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
            catch (Throwable throwable)
            {
                logger.log(Level.WARNING, "Problem processing start tag", throwable);
            }
        }

        @Override
        public void endElement(String uri, String localName, String qName)
            throws SAXException
        {
            try
            {
                if ((localName != null) && localName.equals(VALUE_TAGNAME) && (uri != null) && uri.equals(SPREADSHEETML_NAMESPACE))
                {
                    if (_cellType == null)
                    {
                        try
                        {
                        	int           styleIndex   = Integer.parseInt(_cellStyle);
                            XSSFCellStyle style        = _stylesTable.getStyleAt(styleIndex);
                            short         formatIndex  = style.getDataFormat();
                            String        formatString = style.getDataFormatString();
                            if (formatString == null)
                                formatString = BuiltinFormats.getBuiltinFormat(formatIndex);
                            String text = _formatter.formatRawCellContents(Double.parseDouble(_value.toString()), formatIndex, formatString);
                            _rowMap.put(removeRowNumber(_cellName), text);
                        }
                        catch (NumberFormatException numberFormatException)
                        {
                            logger.log(Level.WARNING, "Failed to parse 'Style' index", numberFormatException);
                        }
                        catch (IndexOutOfBoundsException indexOutOfBoundsException)
                        {
                            logger.log(Level.WARNING, "Failed to find 'Style'", indexOutOfBoundsException);
                        }
                    }
                    else if (_cellType.equals("n"))
                        _rowMap.put(removeRowNumber(_cellName), _value.toString());
                    else if (_cellType.equals("s"))
                    {
                        String sharedStringsTableIndex = _value.toString();
                        try
                        {
                            int index = Integer.parseInt(sharedStringsTableIndex);
                            XSSFRichTextString rtss = new XSSFRichTextString(_sharedStringsTable.getEntryAt(index));
                            _rowMap.put(removeRowNumber(_cellName), rtss.toString());
                        }
                        catch (NumberFormatException numberFormatException)
                        {
                            logger.log(Level.WARNING, "Failed to parse 'Shared Strings Table' index '" + sharedStringsTableIndex + "'", numberFormatException);
                        }
                        catch (IndexOutOfBoundsException indexOutOfBoundsException)
                        {
                            logger.log(Level.WARNING, "Failed to find 'Shared String' - '" + sharedStringsTableIndex + "'", indexOutOfBoundsException);
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
            catch (Throwable throwable)
            {
                logger.log(Level.WARNING, "Problem processing end tag", throwable);
            }
        }

        @Override
        public void characters(char[] characters, int start, int length)
            throws SAXException
        {
        	try
        	{
                _value.append(characters, start, length);
        	}
            catch (Throwable throwable)
            {
                logger.log(Level.WARNING, "Problem processing characters", throwable);
            }
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
            sql.append(",'',");
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
        private StylesTable         _stylesTable;
        private DataFormatter       _formatter;
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
            StylesTable        stylesTable        = xssfReader.getStylesTable();

            XMLReader      workbookParser  = XMLReaderFactory.createXMLReader("org.apache.xerces.parsers.SAXParser");
            ContentHandler workbookHandler = new WorkbookHandler(refIdMap);
            workbookParser.setContentHandler(workbookHandler);

            InputStream workbookInputStream = xssfReader.getWorkbookData();
            InputSource workbookSource        = new InputSource(workbookInputStream);
            workbookParser.parse(workbookSource);
            workbookInputStream.close();

            XMLReader      sheetParser  = XMLReaderFactory.createXMLReader("org.apache.xerces.parsers.SAXParser");
            ContentHandler sheetHandler = new SheetHandler(tableName, sharedStringsTable, stylesTable);
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
        String tableName = "companieshouse_" + UUID.randomUUID().toString().replace('-', '_');

        try
        {
            StringBuffer createCommandBuffer = new StringBuffer();

            createCommandBuffer.append("CREATE TABLE ");
            createCommandBuffer.append(tableName);
            createCommandBuffer.append("\n (\n");
            createCommandBuffer.append("    companyname TEXT,\n");
            createCommandBuffer.append("    companynumber TEXT,\n");
            createCommandBuffer.append("    regaddresscareof TEXT,\n");
            createCommandBuffer.append("    regaddresspobox TEXT,\n");
            createCommandBuffer.append("    regaddressaddressline1 TEXT,\n");
            createCommandBuffer.append("    regaddressaddressline2 TEXT,\n");
            createCommandBuffer.append("    regaddressposttown TEXT,\n");
            createCommandBuffer.append("    regaddresscounty TEXT,\n");
            createCommandBuffer.append("    regaddresscountry TEXT,\n");
            createCommandBuffer.append("    regaddresspostcode TEXT,\n");
            createCommandBuffer.append("    companycategory TEXT,\n");
            createCommandBuffer.append("    companystatus TEXT,\n");
            createCommandBuffer.append("    countryoforigin TEXT,\n");
            createCommandBuffer.append("    dissolutiondate TEXT,\n");
            createCommandBuffer.append("    incorporationdate TEXT,\n");
            createCommandBuffer.append("    accountsaccountrefday TEXT,\n");
            createCommandBuffer.append("    accountsaccountrefmonth TEXT,\n");
            createCommandBuffer.append("    accountsnextduedate TEXT,\n");
            createCommandBuffer.append("    accountslastmadeupdate TEXT,\n");
            createCommandBuffer.append("    accountsaccountcategory TEXT,\n");
            createCommandBuffer.append("    returnsnextduedate TEXT,\n");
            createCommandBuffer.append("    returnslastmadeupdate TEXT,\n");
            createCommandBuffer.append("    mortgagesnummortcharges TEXT,\n");
            createCommandBuffer.append("    mortgagesnummortoutstanding TEXT,\n");
            createCommandBuffer.append("    mortgagesnummortpartsatisfied TEXT,\n");
            createCommandBuffer.append("    mortgagesnummortsatisfied TEXT,\n");
            createCommandBuffer.append("    siccodesictext_1 TEXT,\n");
            createCommandBuffer.append("    siccodesictext_2 TEXT,\n");
            createCommandBuffer.append("    siccodesictext_3 TEXT,\n");
            createCommandBuffer.append("    siccodesictext_4 TEXT,\n");
            createCommandBuffer.append("    limitedpartnershipsnumgenpartners TEXT,\n");
            createCommandBuffer.append("    limitedpartnershipsnumlimpartners TEXT,\n");
            createCommandBuffer.append("    uri TEXT,\n");
            createCommandBuffer.append("    previousname_1condate TEXT,\n");
            createCommandBuffer.append("    previousname_1companyname TEXT,\n");
            createCommandBuffer.append("    previousname_2condate TEXT,\n");
            createCommandBuffer.append("    previousname_2companyname TEXT,\n");
            createCommandBuffer.append("    previousname_3condate TEXT,\n");
            createCommandBuffer.append("    previousname_3companyname TEXT,\n");
            createCommandBuffer.append("    previousname_4condate TEXT,\n");
            createCommandBuffer.append("    previousname_4companyname TEXT,\n");
            createCommandBuffer.append("    previousname_5condate TEXT,\n");
            createCommandBuffer.append("    previousname_5companyname TEXT,\n");
            createCommandBuffer.append("    previousname_6condate TEXT,\n");
            createCommandBuffer.append("    previousname_6companyname TEXT,\n");
            createCommandBuffer.append("    previousname_7condate TEXT,\n");
            createCommandBuffer.append("    previousname_7companyname TEXT,\n");
            createCommandBuffer.append("    previousname_8condate TEXT,\n");
            createCommandBuffer.append("    previousname_8companyname TEXT,\n");
            createCommandBuffer.append("    previousname_9condate TEXT,\n");
            createCommandBuffer.append("    previousname_9companyname TEXT,\n");
            createCommandBuffer.append("    previousname_10condate TEXT,\n");
            createCommandBuffer.append("    previousname_10companyname TEXT,\n");
            createCommandBuffer.append("    cleancompanyname TEXT,\n");
            createCommandBuffer.append("    id BIGSERIAL NOT NULL,\n");
            createCommandBuffer.append("    CONSTRAINT ");
            createCommandBuffer.append(tableName);
            createCommandBuffer.append("_pkey PRIMARY KEY (id)\n");
            createCommandBuffer.append(") WITH (OIDS=FALSE);");

            Statement createStatement = _connection.createStatement();

            createStatement.executeUpdate(createCommandBuffer.toString());

            createStatement.close();

            StringBuffer nameIndexCommandBuffer = new StringBuffer();
            
            nameIndexCommandBuffer.append("CREATE INDEX ");
            nameIndexCommandBuffer.append(tableName);
            nameIndexCommandBuffer.append("_companyname_idx ON ");
            nameIndexCommandBuffer.append(tableName);
            nameIndexCommandBuffer.append(" USING btree (companyname COLLATE pg_catalog.\"default\");");

            Statement nameIndexStatement = _connection.createStatement();

            nameIndexStatement.executeUpdate(nameIndexCommandBuffer.toString());

            nameIndexStatement.close();

            StringBuffer numberIndexCommandBuffer = new StringBuffer();

            numberIndexCommandBuffer.append("CREATE INDEX ");
            numberIndexCommandBuffer.append(tableName);
            numberIndexCommandBuffer.append("_companynumber_idx ON ");
            numberIndexCommandBuffer.append(tableName);
            numberIndexCommandBuffer.append(" USING btree (companynumber COLLATE pg_catalog.\"default\");");

            Statement numberIndexStatement = _connection.createStatement();

            numberIndexStatement.executeUpdate(numberIndexCommandBuffer.toString());

            numberIndexStatement.close();

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
