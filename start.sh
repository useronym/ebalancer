#!/bin/bash

erl -pa ebin deps/evc/ebin \
    -boot start_sasl \
    -name ebalancer`date +%S` \
    -s ebalancer_app start ebalancer
