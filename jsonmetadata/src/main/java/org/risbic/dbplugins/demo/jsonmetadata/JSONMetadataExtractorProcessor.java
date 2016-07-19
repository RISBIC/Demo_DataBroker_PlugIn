/*
 * Copyright (c) 2014-2015, Arjuna Technologies Limited, Newcastle-upon-Tyne, England. All rights reserved.
 */

package org.risbic.dbplugins.demo.jsonmetadata;

import java.net.URI;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.naming.InitialContext;
import com.arjuna.databroker.data.DataConsumer;
import com.arjuna.databroker.data.DataFlow;
import com.arjuna.databroker.data.DataProcessor;
import com.arjuna.databroker.data.DataProvider;
import com.arjuna.databroker.data.jee.annotation.DataConsumerInjection;
import com.arjuna.databroker.data.jee.annotation.DataProviderInjection;
import com.arjuna.databroker.data.jee.annotation.PostConfig;
import com.arjuna.databroker.data.jee.annotation.PostCreated;
import com.arjuna.databroker.data.jee.annotation.PostRecovery;
import com.arjuna.databroker.data.jee.annotation.PreConfig;
import com.arjuna.databroker.data.jee.annotation.PreDelete;
import com.arjuna.databroker.metadata.MetadataContentStore;
import com.arjuna.dbutils.metadata.json.generator.JSONArrayMetadataGenerator;

public class JSONMetadataExtractorProcessor implements DataProcessor
{
    private static final Logger logger = Logger.getLogger(JSONMetadataExtractorProcessor.class.getName());

    public static final String METADATABLOB_ID_PROPNAME = "Metadata Blog ID";

    public JSONMetadataExtractorProcessor()
    {
        logger.log(Level.FINE, "JSONMetadataExtractorProcessor");

        try
        {
            _metadataContentStore = (MetadataContentStore) new InitialContext().lookup("java:global/databroker/metadata-store-1.0.0/MetadataContentFilesystemStore");
        }
        catch (Throwable throwable)
        {
            logger.log(Level.WARNING, "JSONMetadataExtractorProcessor: no metadataContentStore found", throwable);
        }
    }

    public JSONMetadataExtractorProcessor(String name, Map<String, String> properties)
    {
        logger.log(Level.FINE, "JSONMetadataExtractorProcessor: " + name + ", " + properties);

        _name       = name;
        _properties = properties;

        try
        {
            _metadataContentStore = (MetadataContentStore) new InitialContext().lookup("java:global/databroker/metadata-store-1.0.0/MetadataContentFilesystemStore");
        }
        catch (Throwable throwable)
        {
            logger.log(Level.WARNING, "JSONMetadataExtractorProcessor: no metadataContentStore found", throwable);
        }
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
    @PostRecovery
    @PostConfig
    public void startup()
    {
        _metadataId = _properties.get(METADATABLOB_ID_PROPNAME);
    }

    @PreConfig
    @PreDelete
    public void shutdown()
    {
        _metadataId = null;
    }

    public void consume(byte[] data)
    {
        try
        {
            URI rdfURI = URI.create("http://rdf.arjuna.com/json/" + _metadataId);
            JSONArrayMetadataGenerator jsonArrayMetadataGenerator = new JSONArrayMetadataGenerator();
            String rdf = jsonArrayMetadataGenerator.generateJSONMetadata(rdfURI, data);
            _metadataContentStore.createOverwrite(_metadataId, rdf);

            _dataProvider.produce(data);
        }
        catch (Throwable throwable)
        {
            logger.log(Level.WARNING, "Problem extracting metadata", throwable);
        }
    }

    @Override
    public Collection<Class<?>> getDataConsumerDataClasses()
    {
        Set<Class<?>> dataConsumerDataClasses = new HashSet<Class<?>>();

        dataConsumerDataClasses.add(byte[].class);

        return dataConsumerDataClasses;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> DataConsumer<T> getDataConsumer(Class<T> dataClass)
    {
        if (byte[].class.isAssignableFrom(dataClass))
            return (DataConsumer<T>) _dataConsumer;
        else
            return null;
    }

    @Override
    public Collection<Class<?>> getDataProviderDataClasses()
    {
        Set<Class<?>> dataProviderDataClasses = new HashSet<Class<?>>();

        dataProviderDataClasses.add(byte[].class);

        return dataProviderDataClasses;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> DataProvider<T> getDataProvider(Class<T> dataClass)
    {
        if (byte[].class.isAssignableFrom(dataClass))
            return (DataProvider<T>) _dataProvider;
        else
            return null;
    }

    public String _metadataId;

    private String               _name;
    private Map<String, String>  _properties;
    private DataFlow             _dataFlow;
    @DataConsumerInjection(methodName="consume")
    private DataConsumer<byte[]> _dataConsumer;
    @DataProviderInjection
    private DataProvider<byte[]> _dataProvider;

    private MetadataContentStore _metadataContentStore;
}
