#!/bin/bash

java -jar -Dspring.profiles.active=partycontact target/partycontact-rest-1.0.jar --spring.config.location=file:config/ 
