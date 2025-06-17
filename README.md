Kafka-Topic-Config
===========



### Validate Topic-Config-File

the binary can be called with the '-test' parameter. 
when this parameter is used a kafka cluster with 3 nodes is started with docker-compose and all given files are executed and the results are tested. the logs show the changes made.
the parameter is interpreted as a comma seperated list of yaml file locations.
when this parameter is used, the enable_kubernetes_pod_restart config field is set to false. other config values ar read from the config-file and environment as normal.

example:
```
./kafka-topic-config -test=old.yaml,new.yaml
```