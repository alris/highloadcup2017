#!/bin/bash

echo "Starting test!"

SIMPLE="-test"
SMOKE="-concurrent $2 -time $3s"

[[ "$1" == "smoke" ]] && PARAMS=$SMOKE || PARAMS=$SIMPLE

echo "Mode: $PARAMS"

./highloadcup_tester -addr http://127.0.0.1:8080 -hlcupdocs ./hlcupdocs/data/TRAIN/ $PARAMS -phase 1
#./highloadcup_tester -addr http://127.0.0.1:8080 -hlcupdocs ./hlcupdocs/data/FULL/ $PARAMS -phase 1
