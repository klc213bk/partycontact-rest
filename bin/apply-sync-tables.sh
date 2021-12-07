#!/bin/bash

#curl -X POST http://localhost:9102/logminer/applyLogminerSync -H 'Content-Type: application/json' -d '{"resetOffset":true,"startScn":null, "applyOrDrop":1, "tableListStr":"TGLMINER.T_POLICY_HOLDER"}'

curl -X POST http://localhost:9201/partycontact/applySyncTables