#!/bin/bash

echo "loading All Data"
curl -X POST http://localhost:9201/partycontact/loadAllData

sleep 5

echo "addPrimaryKey"
curl -X POST http://localhost:9201/partycontact/addPrimaryKey

sleep 5

echo "create indexes"
curl -X POST http://localhost:9201/partycontact/createIndexes


