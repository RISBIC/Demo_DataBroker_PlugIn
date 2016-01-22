/*
 * Copyright (c) 2014-2015, Arjuna Technologies Limited, Newcastle-upon-Tyne, England. All rights reserved.
 */

package org.risbic.dbplugins.demo;

import java.util.Collections;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.ejb.EJB;
import javax.ejb.Singleton;
import javax.ejb.Startup;
import com.arjuna.databroker.data.DataFlowNodeFactory;
import com.arjuna.databroker.data.DataFlowNodeFactoryInventory;
import org.risbic.dbplugins.demo.spreadsheet2jdbc.CompaniesHouseSpreadsheet2JDBCProcessorFactory;
import org.risbic.dbplugins.demo.spreadsheet2jdbc.LibrarySpreadsheet2JDBCProcessorFactory;
import org.risbic.dbplugins.demo.spreadsheetmetadata.SpreadsheetMetadataExtractorProcessorFactory;
import org.risbic.dbplugins.demo.spreadsheetmetadata.ExtraSpreadsheetMetadataExtractorProcessorFactory;
import org.risbic.dbplugins.demo.jsonmetadata.JSONMetadataExtractorProcessorFactory;
import org.risbic.dbplugins.demo.jsonmetadata.JSONMetadataPreservingExtractorProcessorFactory;
import org.risbic.dbplugins.demo.jsonmetadata.ExtraJSONMetadataExtractorProcessorFactory;
import org.risbic.dbplugins.demo.jsonmetadata.ExtraJSONMetadataPreservingExtractorProcessorFactory;

@Startup
@Singleton
public class DemoDataFlowNodeFactoriesSetup
{
    @PostConstruct
    public void setup()
    {
        DataFlowNodeFactory companiesHouseSpreadsheet2JDBCProcessorFactory       = new CompaniesHouseSpreadsheet2JDBCProcessorFactory("Companies House Spreadsheet to JDBC Processor Factory", Collections.<String, String>emptyMap());
        DataFlowNodeFactory librarySpreadsheet2JDBCProcessorFactory              = new LibrarySpreadsheet2JDBCProcessorFactory("Library Spreadsheet to JDBC Processor Factory", Collections.<String, String>emptyMap());
        DataFlowNodeFactory spreadsheetMetadataExtractorProcessorFactory         = new SpreadsheetMetadataExtractorProcessorFactory("Spreadsheet Metadata Extractor Processor Factory", Collections.<String, String>emptyMap());
        DataFlowNodeFactory extraSpreadsheetMetadataExtractorProcessorFactory    = new ExtraSpreadsheetMetadataExtractorProcessorFactory("Extra Spreadsheet Metadata Extractor Processor Factory", Collections.<String, String>emptyMap());
        DataFlowNodeFactory jsonMetadataExtractorProcessorFactory                = new JSONMetadataExtractorProcessorFactory("JSON Metadata Extractor Processor Factory", Collections.<String, String>emptyMap());
        DataFlowNodeFactory extraJSONMetadataExtractorProcessorFactory           = new ExtraJSONMetadataExtractorProcessorFactory("Extra JSON Metadata Extractor Processor Factory", Collections.<String, String>emptyMap());
        DataFlowNodeFactory jsonMetadataPreservingExtractorProcessorFactory      = new JSONMetadataPreservingExtractorProcessorFactory("JSON Metadata Preserving Extractor Processor Factory", Collections.<String, String>emptyMap());
        DataFlowNodeFactory extraJSONMetadataPreservingExtractorProcessorFactory = new ExtraJSONMetadataPreservingExtractorProcessorFactory("Extra JSON Metadata Preserving Extractor Processor Factory", Collections.<String, String>emptyMap());

        _dataFlowNodeFactoryInventory.addDataFlowNodeFactory(companiesHouseSpreadsheet2JDBCProcessorFactory);
        _dataFlowNodeFactoryInventory.addDataFlowNodeFactory(librarySpreadsheet2JDBCProcessorFactory);
        _dataFlowNodeFactoryInventory.addDataFlowNodeFactory(spreadsheetMetadataExtractorProcessorFactory);
        _dataFlowNodeFactoryInventory.addDataFlowNodeFactory(extraSpreadsheetMetadataExtractorProcessorFactory);
        _dataFlowNodeFactoryInventory.addDataFlowNodeFactory(jsonMetadataExtractorProcessorFactory);
        _dataFlowNodeFactoryInventory.addDataFlowNodeFactory(extraJSONMetadataExtractorProcessorFactory);
        _dataFlowNodeFactoryInventory.addDataFlowNodeFactory(jsonMetadataPreservingExtractorProcessorFactory);
        _dataFlowNodeFactoryInventory.addDataFlowNodeFactory(extraJSONMetadataPreservingExtractorProcessorFactory);
    }

    @PreDestroy
    public void cleanup()
    {
        _dataFlowNodeFactoryInventory.removeDataFlowNodeFactory("Companies House Spreadsheet to JDBC Processor Factory");
        _dataFlowNodeFactoryInventory.removeDataFlowNodeFactory("Library Spreadsheet to JDBC Processor Factory");
        _dataFlowNodeFactoryInventory.removeDataFlowNodeFactory("Spreadsheet Metadata Extractor Processor Factory");
        _dataFlowNodeFactoryInventory.removeDataFlowNodeFactory("Extra Spreadsheet Metadata Extractor Processor Factory");
        _dataFlowNodeFactoryInventory.removeDataFlowNodeFactory("JSON Metadata Extractor Processor Factory");
        _dataFlowNodeFactoryInventory.removeDataFlowNodeFactory("Extra JSON Metadata Extractor Processor Factory");
        _dataFlowNodeFactoryInventory.removeDataFlowNodeFactory("JSON Metadata Preserving Extractor Processor Factory");
        _dataFlowNodeFactoryInventory.removeDataFlowNodeFactory("Extra JSON Metadata Preserving Extractor Processor Factory");
    }

    @EJB(lookup="java:global/databroker/data-core-jee/DataFlowNodeFactoryInventory")
    private DataFlowNodeFactoryInventory _dataFlowNodeFactoryInventory;
}
