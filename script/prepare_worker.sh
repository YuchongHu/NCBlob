#!/bin/bash
# cd /root/tbr/script
file_path="./iplist.txt"
export user_name="root"

readarray -t iplist < "$file_path"

mount_archive() {
    local line=$1
    if [[ $# -ne 0 ]]; then
        ssh $user_name@$line "mkdir -p /archive && mount /dev/nvme0n1 /archive"
    fi
}
export -f mount_archive

mount_disk() {
    local line=$1
    if [[ $# -ne 0 ]]; then
        ssh $user_name@$line "mkdir -p /data/disk3 && mount /dev/nvme3n1 /data/disk3 && chmod o+wrx -R /data/"
        ssh $user_name@$line "mkdir -p /data/disk1 && mount /dev/nvme1n1 /data/disk1"
    fi
}
export -f mount_disk

backup() {
    local line=$1
    if [[ $# -ne 0 ]]; then
        ssh $user_name@$line "rsync -az --delete --progress --partial /data/disk3/worker /archive/worker"
        echo "Backup done"
    fi
}
export -f backup

restore() {
    local line=$1
    if [[ $# -ne 0 ]]; then
        ssh $user_name@$line "rsync -az --delete --progress --partial /archive/worker /data/disk3/worker" 2>&1
        echo "Restore done"
    fi
}

kill_worker() {
    local line=$1
    if [[ $# -ne 0 ]]; then
        ssh $user_name@$line "killall -SIGTERM data_worker"
        echo "Worker killed"
    fi
}
export -f kill_worker

flushall() {
    local line=$1
    if [[ $# -ne 0 ]]; then
        ssh $user_name@$line "redis-cli -a gc123456. flushall"
        echo "Redis flushed"
    fi
}
export -f flushall

cp_assets() {

    src_bin="../bin/Release/data_worker"
    src_sh="./launch_worker.sh"
    src_cfg="../worker_cfg.toml"

    tgt_bin="/root/worker/data_worker"
    tgt_sh="/root/worker/launch_worker.sh"
    tgt_cfg="/root/worker/worker_cfg.toml"
    local line=$1
    if [[ $# -ne 0 ]]; then
        ssh $user_name@$line "killall -SIGTERM data_worker"
        ssh $user_name@$line "mkdir -p $(dirname $tgt_bin) && mkdir -p $(dirname $tgt_sh) && mkdir -p $(dirname $tgt_cfg)"
        scp -r $src_bin $user_name@$line:$tgt_bin
        scp -r $src_sh $user_name@$line:$tgt_sh
        scp -r $src_cfg $user_name@$line:$tgt_cfg
    fi
}
export -f cp_assets

launch_worker() {
    local line=$1
    if [[ $# -ne 0 ]]; then
        ssh $user_name@$line "date && cd /root/worker && bash ./launch_worker.sh" 2>&1
    fi
}
export -f launch_worker

restart_redis() {
    local line=$1
    if [[ $# -ne 0 ]]; then
        ssh $user_name@$line "echo 1 > /proc/sys/vm/overcommit_memory"
        ssh $user_name@$line "systemctl restart redis"
    fi
}
export -f restart_redis

lsblk() {
    local line=$1
    if [[ $# -ne 0 ]]; then
        ssh $user_name@$line "lsblk"
    fi
}
export -f lsblk

work() {
    # mount_archive $1
    # kill_worker $1
    cp_assets $1
    launch_worker $1
    # flushall $1
    # backup $1
    # restore $1
    # mount_disk $1
    # restart_redis $1
    # lsblk $1
}
export -f work

parallel -j 20 --tmpdir /mnt/tmpfs  --results ../var/par-log work ::: "${iplist[@]}"
