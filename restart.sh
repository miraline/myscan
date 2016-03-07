#!/bin/sh

killall myscan 2>/dev/null
sleep 0.1
killall -9 myscan 2>/dev/null
sleep 0.1
myscan >out.log 2>err.log &
