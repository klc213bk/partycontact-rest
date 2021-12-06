#!/bin/bash

curl -X POST http://localhost:9201/partycontact/createIndex/ADDRESS_1 &

curl -X POST http://localhost:9201/partycontact/createIndex/EMAIL &

curl -X POST http://localhost:9201/partycontact/createIndex/MOBILE_TEL &

curl -X POST http://localhost:9201/partycontact/createIndex/CERTI_CODE &

curl -X POST http://localhost:9201/partycontact/createIndex/POLICY_ID &

curl -X POST http://localhost:9201/partycontact/createIndex/UPDATE_TIMESTAMP &

curl -X POST http://localhost:9201/partycontact/createIndex/ROLE_SCN &

curl -X POST http://localhost:9201/partycontact/createIndex/ADDR_SCN &

