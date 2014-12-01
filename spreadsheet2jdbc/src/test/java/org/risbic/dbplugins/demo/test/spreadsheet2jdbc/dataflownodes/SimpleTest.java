/*
 * Copyright (c) 2013-2014, Arjuna Technologies Limited, Newcastle-upon-Tyne, England. All rights reserved.
 */

package org.risbic.dbplugins.demo.test.spreadsheet2jdbc.dataflownodes;

import java.io.File;
import java.util.Collections;
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
    	String              name              = "XSSF Data Processor";
    	Map<String, String> properties        = Collections.emptyMap();
        Spreadsheet2JDBCProcessor   xssfDataProcessor = new Spreadsheet2JDBCProcessor(name, properties);

        DataFlowNodeLifeCycleControl.processCreatedDataFlowNode(xssfDataProcessor, null);
        
        File file = new File("Test01.xlsx");

        ((ObserverDataConsumer<File>) xssfDataProcessor.getDataConsumer(File.class)).consume(null, file);
    }
}
