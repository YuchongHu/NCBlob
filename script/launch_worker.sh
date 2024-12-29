#!/bin/bash
cd /root/worker/
pkill data_worker
redis-cli -a gc123456. flushall 
redis-cli -a gc123456. config set slave-read-only no
# rm -rf /root/data/disk3/worker
# mkdir -p /root/data/disk3/worker
./data_worker ./worker_cfg.toml