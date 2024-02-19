#!/bin/sh

kill -9 $(ps S | grep sync.sh | awk '{print $1}')
