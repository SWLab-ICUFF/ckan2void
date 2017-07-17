@echo off
call mvn clean install
cd .\target
java -cp ckan2void-1.0.0-SNAPSHOT.jar;dependency\* Main %1