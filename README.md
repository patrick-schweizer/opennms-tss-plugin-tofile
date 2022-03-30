# Time Series Storage - InMemory [![CircleCI](https://circleci.com/gh/opennms-forge/opennms-tss-plugin-inmemory.svg?style=svg)](https://circleci.com/gh/opennms-forge/opennms-tss-plugin-inmemory)

This plugin exposes a simple implementation of the TimeSeriesStorage interface.
It writes the data into a file.
It can be used in OpenNMS to store and retrieve timeseries data.

This implementation is meant to be a demonstration of how to implement the interface and for testing purposes.
It is not meant for production use.

### Usage
* compile: ``mvn install``
* activation: Enable the timeseries integration layer: see [documentation](https://docs.opennms.org/opennms/releases/26.1.0/guide-admin/guide-admin.html#ga-opennms-operation-timeseries)
* activate in Karaf shell: ``bundle:install -s mvn:org.opennms.plugins.tss/inmemory/2.0.0-SNAPSHOT``
* show statistics in Karaf shell: ``opennms-tss-inmemory:stats``

  
 



