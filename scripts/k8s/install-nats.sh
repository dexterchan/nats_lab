#!/bin/sh

helm repo add nats https://nats-io.github.io/k8s/helm/charts/
namespace=nats-eventbus
CURPATH=$(dirname $0)
kubectl create ns $namespace
kubectl apply -f ${CURPATH}/nats-persistant-claim.yaml 

helm upgrade --install my-nats nats/nats \
            --create-namespace --wait \
            --namespace=${namespace} \
            --values ${CURPATH}/values.yaml 
kubectl apply -f ${CURPATH}/lb-nats.yaml