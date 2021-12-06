#!/bin/bash

java -jar -Dspring.profiles.active=dev target/partycontact-rest-1.0.jar --spring.config.location=file:config/ 
