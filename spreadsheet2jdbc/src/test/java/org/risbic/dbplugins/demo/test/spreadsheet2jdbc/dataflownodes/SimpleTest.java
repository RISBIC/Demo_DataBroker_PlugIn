/*
 * Copyright (c) 2014-2015, Arjuna Technologies Limited, Newcastle-upon-Tyne, England. All rights reserved.
 */

package org.risbic.dbplugins.demo.test.spreadsheet2jdbc.dataflownodes;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.junit.Test;
import org.risbic.dbplugins.demo.spreadsheet2jdbc.CompaniesHouseSpreadsheet2JDBCProcessor;
import static org.junit.Assert.*;
import com.arjuna.databroker.data.connector.ObserverDataConsumer;
import com.arjuna.databroker.data.core.DataFlowNodeLifeCycleControl;
import com.arjuna.dbutilities.testsupport.dataflownodes.lifecycle.TestJEEDataFlowNodeLifeCycleControl;

public class SimpleTest
{
    @Test
    public void simpleInvocation()
    {
        DataFlowNodeLifeCycleControl dataFlowNodeLifeCycleControl = new TestJEEDataFlowNodeLifeCycleControl();

        String              name       = "XSSF Data Processor";
        Map<String, String> properties = new HashMap<String, String>();
        properties.put(CompaniesHouseSpreadsheet2JDBCProcessor.DATABASE_CONNECTIONURL_PROPNAME, "jdbc:postgresql://localhost:5432/databroker");
        properties.put(CompaniesHouseSpreadsheet2JDBCProcessor.DATABASE_USERNAME_PROPNAME, "username");
        properties.put(CompaniesHouseSpreadsheet2JDBCProcessor.DATABASE_PASSWORD_PROPNAME, "password");
        CompaniesHouseSpreadsheet2JDBCProcessor xssfDataProcessor = new CompaniesHouseSpreadsheet2JDBCProcessor(name, properties);

        dataFlowNodeLifeCycleControl.completeCreationAndActivateDataFlowNode(UUID.randomUUID().toString(), xssfDataProcessor, null);
        
        File file = new File("Test01.xlsx");

        ((ObserverDataConsumer<File>) xssfDataProcessor.getDataConsumer(File.class)).consume(null, file);
        
        dataFlowNodeLifeCycleControl.removeDataFlowNode(xssfDataProcessor);
    }
}
