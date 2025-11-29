# node-refresh-operator
Node refresh automation project:

Build and Deploy:

make all

Create Example Resource:

kubectl apply -f examples/noderefresh-example.yaml

Monitor Operations:

kubectl get noderefreshes -w
kubectl logs -f deployment/node-refresh-operator -n node-refresh-operator
