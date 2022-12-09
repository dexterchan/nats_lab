#!/bin/sh
CURPATH=$(dirname $0)

helm delete my-nats -n nats-eventbus
kubectl delete -f ${CURPATH}/nats-persistant-claim.yaml
kubectl delete -f ${CURPATH}/lb-nats.yaml