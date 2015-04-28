#!/bin/bash

erl -pa ebin deps/*/ebin \
    -boot start_sasl \
    -name ebalancer \
    -s ebalancer_app
