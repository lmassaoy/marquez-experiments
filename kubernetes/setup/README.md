# Setup

## Kubernetes (Local Environment)

If you're running your local environment on Windows and don't have a Kubernetes cluster running yet, please check the following [link](https://phoenixnap.com/kb/kubernetes-on-windows)

## Setup on Mac OS
### Have a Docker environment ready
Follow these [instructions](https://docs.docker.com/desktop/install/mac-install/)

### Install kubectl
```
brew install kubectl
```

### Install helm
```
brew install helm
```

### Install minikube
```
brew install minikube
```

## Starting Kubernetes environment (minikube)
```
minikube start
minikube addons enable ingress
kubectl create namespace marquez
```

### Setting up Kubernetes Dashboard
```
kubectl apply -f https://raw.githubusercontent.com/kubernetes/dashboard/v2.7.0/aio/deploy/recommended.yaml
```

### Prepare the admin user for logging into the Kubernetes Dashboard
```
kubectl apply -f kubernetes/setup/cluster_admin_user/dashboard-adminuser.yaml
```
```
kubectl apply -f kubernetes/setup/cluster_admin_user/dashboard-adminuser-clusterrolebind.yaml
```
```
kubectl apply -f kubernetes/setup/cluster_admin_user/dashboard-adminuser-secret.yaml
```

### Collect token to access Kubernetes Dashboard
```
kubectl -n kube-system get secret admin-user --namespace=kubernetes-dashboard -o jsonpath='{.data.token}' | base64 --decode
```

### Run the Dashboard
```
kubectl proxy
```
Access in your browser the Dashboard [link](http://localhost:8001/api/v1/namespaces/kubernetes-dashboard/services/https:kubernetes-dashboard:/proxy/)

Enter the token collected