minikube start --memory 6144 --cpus 3
minikube addons enable ingress
minikube addons enable metrics-server
kubectl create namespace marquez