
## Configuration
```commandline
kubectl apply -f config_baskervillehall.yaml
```
## Retain Storage Class
```commandline
kubectl apply -f ./deployment/storage_class.yaml
```
## ASN database
* Download the updated bad-asn-list.csv from <https://github.com/brianhama/bad-asn-list/blob/master/bad-asn-list.csv>
* unzip the file to deployment/bad_asn/bad-asn-list.csv
* create docker image
```
cd deployment/bad_asn
docker build -t equalitie/baskervillehall_bad_asn:latest .
docker push equalitie/baskervillehall_bad_asn:latest
cd ../..
```

## VPN database
* Clone the repo
```commandline
cd deployment/vpn_asn
git clone git@github.com:NullifiedCode/ASN-Lists.git
docker build -t equalitie/baskervillehall_vpn_asn:latest .
docker push equalitie/baskervillehall_vpn_asn:latest
cd ../..
```

### Building
```
docker buildx build --platform linux/amd64 -t equalitie/baskervillehall:base .

```

```
docker buildx build --platform linux/amd64 -f ./Dockerfile_latest . -t equalitie/baskervillehall:latest
docker push equalitie/baskervillehall:latest
```

### Pipelines deployment
kubectl apply -f session_deployment.yaml
kubectl apply -f prediction_deployment.yaml
kubectl apply -f trainer_deployment.yaml
kubectl apply -f storage_deployment.yaml

## Jupyterhub

### Build image
```commandline
docker buildx build --platform linux/amd64 -f ./Dockerfile_jupyter_base . -t equalitie/baskervillehall:jupyterbase
docker push equalitie/baskervillehall:jupyterbase

docker buildx build --platform linux/amd64 -f ./Dockerfile_jupyter . -t equalitie/baskervillehall:jupyter
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
  --version=4.2.0 \
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
Please, use user `admin` with an empty password.

### Postgres deployment
kubectl apply -f deployment/postgres/postgres-baskervillehall-secret.yaml
kubectl apply -f deployment/postgres/postgres-baskervillehall-pv.yaml
kubectl apply -f deployment/postgres/postgres-baskervillehall-pvc.yaml
kubectl apply -f deployment/postgres/postgres-baskervillehall.yaml
kubectl apply -f deployment/postgres/postgres-baskervillehall-service.yaml

kubectl port-forward service/postgres-baskervillehall 5433:5432

