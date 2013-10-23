elastic-cleaner
=================

A small utility which removes empty indexes from Elasticsearch. Developed as Elasticseach Java Remote API learning excersize.
I use it to remove empty logstash indexes, left by Elasticsearch after housekeeping (because of _ttl enabled in logstash mapping).

You get a shaded uber-jar after maven build, use clean.bat to start.

Environment variables:

* debug.enable - log level set to DEBUG if enabled;
* cluster.name - name of your Elasticsearch cluster;
* host.name - where is your Elasticsearch cluster running; 
* port.number - transport port number;
* index.name - substring to match in index name (WARNING! It can delete your indexes if you made a mistake here);
