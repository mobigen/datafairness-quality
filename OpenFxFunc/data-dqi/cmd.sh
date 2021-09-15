#!/bin/bash

action=${1}
function="data-dqi"
gateway="192.168.101.112:31113"

if [ "log" = $action ] || [ "delete" = $action ]; then
	cmd="openfx-cli fn ${action} ${function} -g ${gateway}"
elif [ "build" = $action ] || [ "deploy" = $action ]; then
	cmd="openfx-cli fn ${action} -f config.yaml -g ${gateway} -v"
elif [ "list" = $action ]; then
	cmd="kubectl get pods --all-namespaces"
else
	echo "invalid command (log, build, deploy, delete, list)"
fi

$cmd
