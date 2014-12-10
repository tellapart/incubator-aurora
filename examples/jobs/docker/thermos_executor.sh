#!/usr/bin/env bash

if [ "$1" == "--docker" ]; then
	if [ "$2" != "" ]; then
		export LIBPROCESS_IP=$2
	fi

	cd $MESOS_SANDBOX
	exec $MESOS_SANDBOX/thermos_executor.pex --announcer-enable --announcer-ensemble localhost:2181 --execute-as-container --dockerize
else
	exec /home/vagrant/aurora/dist/thermos_executor.pex --announcer-enable --announcer-ensemble localhost:2181
fi

