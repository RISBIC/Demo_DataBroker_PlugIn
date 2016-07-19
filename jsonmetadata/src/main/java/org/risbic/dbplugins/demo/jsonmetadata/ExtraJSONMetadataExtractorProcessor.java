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

public class ExtraJSONMetadataExtractorProcessor implements DataProcessor
{
    private static final Logger logger = Logger.getLogger(ExtraJSONMetadataExtractorProcessor.class.getName());

    public static final String METADATABLOB_ID_PROPNAME = "Metadata Blog ID";
    public static final String LOCATION_PROPNAME        = "Location";

    public ExtraJSONMetadataExtractorProcessor()
    {
        logger.log(Level.FINE, "ExtraJSONMetadataExtractorProcessor");

        try
        {
            _metadataContentStore = (MetadataContentStore) new InitialContext().lookup("java:global/databroker/metadata-store-1.0.0/MetadataContentFilesystemStore");
        }
        catch (Throwable throwable)
        {
            logger.log(Level.WARNING, "ExtraJSONMetadataExtractorProcessor: no metadataContentStore found", throwable);
        }
    }

    public ExtraJSONMetadataExtractorProcessor(String name, Map<String, String> properties)
    {
        logger.log(Level.FINE, "ExtraJSONMetadataExtractorProcessor: " + name + ", " + properties);

        _name       = name;
        _properties = properties;

        try
        {
            _metadataContentStore = (MetadataContentStore) new InitialContext().lookup("java:global/databroker/metadata-store-1.0.0/MetadataContentFilesystemStore");
        }
        catch (Throwable throwable)
        {
            logger.log(Level.WARNING, "ExtraJSONMetadataExtractorProcessor: no metadataContentStore found", throwable);
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
        _location   = _properties.get(LOCATION_PROPNAME);
        if ((_location != null) && "".equals(_location.trim()))
            _location = null;
    }

    @PreConfig
    @PreDelete
    public void shutdown()
    {
        _metadataId = null;
        _location   = null;
    }

    public void consume(Map data)
    {
        try
        {
            Object filename = data.get("filename");
            if ((data.get("resourcename") == null) && (filename != null) && (filename instanceof String))
                data.put("resourcename", filename);
            data.put("resourceformat", "json");
            if (_location != null)
                data.put("location", _location);

            URI rdfURI = URI.create("http://rdf.arjuna.com/json/" + _metadataId);
            JSONArrayMetadataGenerator jsonArrayMetadataGenerator = new JSONArrayMetadataGenerator();
            String rdf = jsonArrayMetadataGenerator.generateJSONArrayMetadata(rdfURI, (Map<String, Object>) data);
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

        dataConsumerDataClasses.add(Map.class);

        return dataConsumerDataClasses;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> DataConsumer<T> getDataConsumer(Class<T> dataClass)
    {
        if (Map.class.isAssignableFrom(dataClass))
            return (DataConsumer<T>) _dataConsumer;
        else
            return null;
    }

    @Override
    public Collection<Class<?>> getDataProviderDataClasses()
    {
        Set<Class<?>> dataProviderDataClasses = new HashSet<Class<?>>();

        dataProviderDataClasses.add(Map.class);

        return dataProviderDataClasses;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> DataProvider<T> getDataProvider(Class<T> dataClass)
    {
        if (Map.class.isAssignableFrom(dataClass))
            return (DataProvider<T>) _dataProvider;
        else
            return null;
    }

    public String _metadataId;
    public String _location;

    private String              _name;
    private Map<String, String> _properties;
    private DataFlow            _dataFlow;
    @DataConsumerInjection(methodName="consume")
    private DataConsumer<Map>   _dataConsumer;
    @DataProviderInjection
    private DataProvider<Map>   _dataProvider;

    private MetadataContentStore _metadataContentStore;
}
