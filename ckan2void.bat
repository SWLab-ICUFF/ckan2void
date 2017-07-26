@echo off
call mvn clean install
cd .\target
java -Xmx4096m -cp ckan2void-1.0.0-SNAPSHOT.jar;dependency\* Main
cd ..