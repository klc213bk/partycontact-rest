#!/bin/bash


curl -X POST http://localhost:9201/partycontact/loadData/T_POLICY_HOLDER &

curl -X POST http://localhost:9201/partycontact/loadData/T_INSURED_LIST &

curl -X POST http://localhost:9201/partycontact/loadData/T_CONTRACT_BENE &

curl -X POST http://localhost:9201/partycontact/loadData/T_POLICY_HOLDER_LOG &

curl -X POST http://localhost:9201/partycontact/loadData/T_INSURED_LIST_LOG &

curl -X POST http://localhost:9201/partycontact/loadData/T_CONTRACT_BENE_LOG &