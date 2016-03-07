#!/bin/bash

cd src/ && rm -f CMakeCache.txt && cmake . && echo;echo;echo && make 2>&1 | head -n 20
