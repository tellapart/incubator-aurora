#!/usr/bin/env bash

if [ "$1" == "--dockerize" ]; then
	cd $MESOS_SANDBOX
	exec $MESOS_SANDBOX/thermos_executor.pex --nosetuid
else
	exec /home/vagrant/aurora/dist/thermos_executor.pex
fi

