#!/bin/bash

erl -pa ebin \
    -boot start_sasl \
    -name ebalancer \
    -s ebalancer_app
