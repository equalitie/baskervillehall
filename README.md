# Baskervillehall
Bot mitigation 

## Configuration
```commandline
kubectl apply -f config_baskervillehall.yaml
```

# Building
```
docker build . -t equalitie/baskervillehall:base
```

```
docker build -f ./Dockerfile_latest . -t equalitie/baskervillehall:latest
docker push equalitie/baskervillehall:latest
```

# Sessions deployment
kubectl apply -f session_deployment.yaml
