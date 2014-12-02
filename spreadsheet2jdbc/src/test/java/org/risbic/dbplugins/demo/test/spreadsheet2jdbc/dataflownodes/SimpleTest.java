/*
 * Copyright (c) 2013-2014, Arjuna Technologies Limited, Newcastle-upon-Tyne, England. All rights reserved.
 */

package org.risbic.dbplugins.demo.test.spreadsheet2jdbc.dataflownodes;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;
import org.risbic.dbplugins.demo.spreadsheet2jdbc.Spreadsheet2JDBCProcessor;
import static org.junit.Assert.*;
import com.arjuna.databroker.data.connector.ObserverDataConsumer;
import com.arjuna.databroker.data.jee.DataFlowNodeLifeCycleControl;

public class SimpleTest
{
    @Test
    public void simpleInvocation()
    {
        String              name       = "XSSF Data Processor";
        Map<String, String> properties = new HashMap<String, String>();
        properties.put(Spreadsheet2JDBCProcessor.DATABASE_CONNECTIONURL_PROPNAME, "jdbc:postgresql://localhost:5432/databroker");
        properties.put(Spreadsheet2JDBCProcessor.DATABASE_USERNAME_PROPNAME, "username");
        properties.put(Spreadsheet2JDBCProcessor.DATABASE_PASSWORD_PROPNAME, "password");
        Spreadsheet2JDBCProcessor xssfDataProcessor = new Spreadsheet2JDBCProcessor(name, properties);

        DataFlowNodeLifeCycleControl.processCreatedDataFlowNode(xssfDataProcessor, null);
        
        File file = new File("Test01.xlsx");

        ((ObserverDataConsumer<File>) xssfDataProcessor.getDataConsumer(File.class)).consume(null, file);
    }
}
