package com.rokhmanov.elasticsearch.util;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

public class IndexCleaner
{    
    private Client client;
    private static Logger logger;
    private final static boolean DEBUG_ENABLE = Boolean.getBoolean("debug.enable"); 
    private final String CLUSTER_NAME = System.getProperty("cluster.name", "elasticsearch");
    private final String HOST_NAME = System.getProperty("host.name", "localhost");
    private final int PORT_NUMBER = Integer.getInteger("port.number", 9300);
    private final String INDEX_NAME = System.getProperty("index.name", "logstash");
    
    public static void main(String[] args)
    {
        logger = Logger.getRootLogger();
        logger.addAppender(new ConsoleAppender(new PatternLayout("%-6r [%p] %c - %m%n")));
        if (!DEBUG_ENABLE)
        {
            logger.setLevel(Level.INFO);    
        }                
        IndexCleaner cleaner = new IndexCleaner();
        cleaner.perform();
    }

    public void perform()
    {
        init();
        Set<String> indexesToClean = getEmptyIndexes();
        clean(indexesToClean);
        shutdown();
    }
    
    
    private void init()
    {
        Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", CLUSTER_NAME).build(); 
        client = new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress(HOST_NAME, PORT_NUMBER));
    }
        
    private Set<String> getEmptyIndexes()
    {
        IndicesStatsRequest indicesStatsRequest = new IndicesStatsRequest();
        indicesStatsRequest.clear().docs(true);
        IndicesStatsResponse indStats = client.admin().indices().stats(indicesStatsRequest).actionGet();
        Map<String, IndexStats> ids = indStats.getIndices();
        Set<String> keys = ids.keySet();
        Set<String> emptyIndexes = new HashSet<String>();
        for (String key : keys)
        {
            if (key.contains(INDEX_NAME))
            {
                IndexStats is = ids.get(key);                
                long numberOfDocs = is.getTotal().docs.getCount();
                logger.debug("For index:" + key + ", number of docs:" + numberOfDocs);
                if (0 == numberOfDocs)
                {
                    emptyIndexes.add(key);                    
                }
            }
        }
        logger.info("Empty Indexes:" + Arrays.toString(emptyIndexes.toArray()));
        return emptyIndexes;
    }
    
    private void clean(Set<String> emptyIds)
    {
        for (String index : emptyIds)
        {
            DeleteIndexResponse resp = client.admin().indices().prepareDelete(index).execute().actionGet();
            logger.debug(resp);
            if (false == resp.isAcknowledged())
            {
                logger.error("Error deleting index " + index);
            }                            
        }
        logger.info("Done.");
    }
    
    private void shutdown()
    {
       client.close(); 
    }
    
}
