cd kubernetes/
helm install marquez . --dependency-update -f values.yaml --namespace marquez --atomic --wait