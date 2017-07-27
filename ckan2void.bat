@echo off
call mvn clean install
cd .\target
java -Xmx2048m -cp ckan2void-0.0.1-SNAPSHOT.jar;dependency\* Main
cd ..