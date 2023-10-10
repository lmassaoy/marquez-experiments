# Initializing Marquez in your Kubernetes cluster

Make sure you have a Kubernetes cluster environment up and running, along Helm prepared.
If you don't have them yet, please check in marquez-experiments\kubernetes\setup\README.md how to setup one.

## Edit your values.yaml

This file contains the parameters for Marquez and its deployments. Check more about it in the official [link](marquez-experiments\kubernetes\setup\README.md)

## Run

Start Marquez by running the `start_marquez_on_k8s.sh` script

### Enabling local interactions through Port-Forward

Forward a local port to the Marquez REST API
```
kubectl port-forward --namespace=marquez svc/marquez 5000:80
```
The Marquez REST API shall be available at http://localhost:5000/api/v1/ ([Marquez OpenAPI docs](https://marquezproject.github.io/marquez/openapi.html))


Forward a local port to the Marquez Web UI
```
kubectl port-forward --namespace=marquez svc/marquez-web 3000:80
```
The Marquez Web UI shall be available at http://localhost:3000

Forward a local port to the PostgreSQL
```
kubectl port-forward --namespace=marquez svc/marquez-postgresql 5432:5432
```
Access the database via pgAdmin


## Stop

Start Marquez by running the `stop_marquez_on_k8s.sh` script

## Additional Thoughts