#!/bin/bash

if [ "$1" == 'balancer' ]; then
    erl -pa ebin -boot start_sasl -name ebalancer -s ebalancer_app start balancer
else
    erl -pa ebin -boot start_sasl -name w`date +%S` -s ebalancer_app start worker
fi
