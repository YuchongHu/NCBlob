#!/bin/bash

cd ~/tbr/
redis-cli flushall
rm -rf ./var/coord_meta/*
./bin/coordinator ./coord_cfg.toml