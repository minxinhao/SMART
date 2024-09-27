#!/bin/bash

addr=$(head -1 ../memcached.conf)
port=$(awk 'NR==2{print}' ../memcached.conf)

echo "Address: ${addr}"
echo "Port: ${port}"

# kill old me
sshpass -p "mxh" ssh ${addr} -o StrictHostKeyChecking=no "cat /tmp/memcached.pid | xargs kill"
echo "kill end"

# launch memcached
sshpass -p "mxh" ssh ${addr} -o StrictHostKeyChecking=no "memcached -u root -l ${addr} -p  ${port} -c 10000 -d -P /tmp/memcached.pid"
sleep 1
echo "launch end"

# init 
echo -e "set serverNum 0 0 1\r\n0\r\nquit\r" | nc ${addr} ${port}
echo -e "set clientNum 0 0 1\r\n0\r\nquit\r" | nc ${addr} ${port}
echo -e "get serverNum\r\nquit\r" | nc ${addr} ${port}
echo -e "get clientNum\r\nquit\r" | nc ${addr} ${port}