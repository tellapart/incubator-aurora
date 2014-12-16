#!/usr/bin/env bash

if [ "$1" == "--docker" ]; then
	cd $MESOS_SANDBOX
	exec $MESOS_SANDBOX/thermos_executor.pex --execute-as-container --dockerize
else
	exec /home/vagrant/aurora/dist/thermos_executor.pex
fi

