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

@Startup
@Singleton
public class DemoDataFlowNodeFactoriesSetup
{
    @PostConstruct
    public void setup()
    {
        DataFlowNodeFactory companiesHouseSpreadsheet2JDBCProcessorFactory = new CompaniesHouseSpreadsheet2JDBCProcessorFactory("Companies House Spreadsheet to JDBC Processor Factory", Collections.<String, String>emptyMap());
        DataFlowNodeFactory librarySpreadsheet2JDBCProcessorFactory        = new LibrarySpreadsheet2JDBCProcessorFactory("Library Spreadsheet to JDBC Processor Factory", Collections.<String, String>emptyMap());

        _dataFlowNodeFactoryInventory.addDataFlowNodeFactory(companiesHouseSpreadsheet2JDBCProcessorFactory);
        _dataFlowNodeFactoryInventory.addDataFlowNodeFactory(librarySpreadsheet2JDBCProcessorFactory);
    }

    @PreDestroy
    public void cleanup()
    {
        _dataFlowNodeFactoryInventory.removeDataFlowNodeFactory("Companies House Spreadsheet to JDBC Processor Factory");
        _dataFlowNodeFactoryInventory.removeDataFlowNodeFactory("Library Spreadsheet to JDBC Processor Factory");
    }

    @EJB(lookup="java:global/databroker/control-core/DataFlowNodeFactoryInventory")
    private DataFlowNodeFactoryInventory _dataFlowNodeFactoryInventory;
}
