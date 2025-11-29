# node-refresh-operator
Node refresh automation project:

Build and Deploy:

make all

Create Example Resource:

kubectl apply -f examples/noderefresh-example.yaml

Monitor Operations:

kubectl get noderefreshes -w
kubectl logs -f deployment/node-refresh-operator -n node-refresh-operator


Key Benifits that are going to be implemented:

 Custom Resource Definition: Complete CRD with all required fields

 Reconciliation Loop: Continuous monitoring and reconciliation

 Safe Pod Eviction: Respects PDBs and application health

 Zero-Downtime Migration: Provisions new nodes before draining old ones

 Retry Mechanism: Automatic retry for failed operations

 Comprehensive Logging: Detailed operation logging

 Status Updates: Real-time status reporting through CRD

 3-Day Schedule: Automated refresh cycles every 3 days

 Health Checks: Application and pod health verification

 RBAC Configuration: Proper security permissions