#!/bin/bash
sudo docker exec -it cassandra1  cqlsh -u cassandra -p cassandra -f schema.cql