# on Windows, run with Git Bash
kubectl apply -f dashboard-adminuser.yaml
kubectl apply -f dashboard-adminuser-clusterrolebind.yaml
kubectl apply -f dashboard-adminuser-secret.yaml
kubectl get secret admin-user -n kubernetes-dashboard -o jsonpath={".data.token"} | base64 -d