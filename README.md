# Baskervillehall
Bot mitigation 

## Configuration
```commandline
kubectl apply -f config_baskervillehall.yaml
```

### Building
```
docker build . -t equalitie/baskervillehall:base
```

```
docker build -f ./Dockerfile_latest . -t equalitie/baskervillehall:latest
docker push equalitie/baskervillehall:latest
```

### Sessions deployment
kubectl apply -f session_deployment.yaml

## Jupyterhub

### Build image
```commandline
docker build -f ./Dockerfile_jupyter . -t equalitie/baskervillehall:jupyter
docker push equalitie/baskervillehall:jupyter
```

### Installation
```commandline
kubectl apply -f jupyter/admin-pvc.yaml
helm repo add jupyter https://hub.jupyter.org/helm-chart/
helm repo update
```

```commandline
helm upgrade --cleanup-on-fail \
  --install jupyter jupyter/jupyterhub \
  --namespace default \
  --version=3.2.1 \
  --values ./jupyter/config.yaml
```

### Jupyterhub modification
```commandline
helm upgrade --cleanup-on-fail \
  jupyter jupyter/jupyterhub \
  --namespace default \
  --version=3.2.1 \
  --values ./jupyter/config.yaml
```

### Jupyterhub forwarding
```commandline
kubectl port-forward service/proxy-public 8080:http
```
Please, use user `admin` with an empth password.

### Postgres deployment
kubectl apply -f deployment/postgres/postgres-baskervillehall-secret.yaml
kubectl apply -f deployment/postgres/postgres-baskervillehall-pv.yaml
kubectl apply -f deployment/postgres/postgres-baskervillehall-pvc.yaml
kubectl apply -f deployment/postgres/postgres-baskervillehall.yaml
kubectl apply -f deployment/postgres/postgres-baskervillehall-service.yaml

kubectl port-forward service/postgres-baskervillehall 5433:5432
