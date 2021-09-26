#!/bin/bash
hadoop fs -mkdir -p /input |
hadoop fs -mkdir -p /output/models/ |
hadoop fs -put ../dataset/2009.csv /input/2009.csv |
hadoop fs -put ../dataset/2010.csv /input/2010.csv |
hadoop fs -put ../dataset/2011.csv /input/2011.csv |
hadoop fs -put ../dataset/2012.csv /input/2012.csv |
hadoop fs -put ../dataset/2013.csv /input/2013.csv |
hadoop fs -put ../dataset/2014.csv /input/2014.csv |
hadoop fs -put ../dataset/2015.csv /input/2015.csv |
hadoop fs -put ../dataset/2016.csv /input/2016.csv |
hadoop fs -put ../dataset/2017.csv /input/2017.csv |
hadoop fs -put ../dataset/2018.csv /input/2018.csv |
hadoop fs -put ../dataset/2019.csv /input/2019.csv |
hadoop fs -put ../dataset/2020.csv /input/2020.csv
