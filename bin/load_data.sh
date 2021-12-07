#!/bin/bash

echo "loading T_POLICY_HOLDER"
curl -X POST http://localhost:9201/partycontact/loadData/T_POLICY_HOLDER &

echo "loading T_INSURED_LIST"
curl -X POST http://localhost:9201/partycontact/loadData/T_INSURED_LIST &

echo "loading T_CONTRACT_BENE"
curl -X POST http://localhost:9201/partycontact/loadData/T_CONTRACT_BENE &

echo "loading T_POLICY_HOLDER_LOG"
curl -X POST http://localhost:9201/partycontact/loadData/T_POLICY_HOLDER_LOG &

echo "loading T_INSURED_LIST_LOG"
curl -X POST http://localhost:9201/partycontact/loadData/T_INSURED_LIST_LOG

echo "loading T_CONTRACT_BENE_LOG"
curl -X POST http://localhost:9201/partycontact/loadData/T_CONTRACT_BENE_LOG

echo "addPrimaryKey"
curl -X POST http://localhost:9201/partycontact/addPrimaryKey

echo "create index ADDRESS_1"
curl -X POST http://localhost:9201/partycontact/createIndex/ADDRESS_1 &

echo "create index EMAIL"
curl -X POST http://localhost:9201/partycontact/createIndex/EMAIL &

echo "create index MOBILE_TEL"
curl -X POST http://localhost:9201/partycontact/createIndex/MOBILE_TEL &

echo "create index CERTI_CODE"
curl -X POST http://localhost:9201/partycontact/createIndex/CERTI_CODE &

echo "create index POLICY_ID"
curl -X POST http://localhost:9201/partycontact/createIndex/POLICY_ID &

echo "create index UPDATE_TIMESTAMP"
curl -X POST http://localhost:9201/partycontact/createIndex/UPDATE_TIMESTAMP &

echo "create index ROLE_SCN"
curl -X POST http://localhost:9201/partycontact/createIndex/ROLE_SCN

echo "create index ADDR_SCN"
curl -X POST http://localhost:9201/partycontact/createIndex/ADDR_SCN



