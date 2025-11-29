import logging
from typing import List, Dict

from kubernetes.client.rest import ApiException

logger = logging.getLogger(__name__)

class PodManager:
    def __init__(self, core_v1, apps_v1, policy_v1):
        self.core_v1 = core_v1
        self.apps_v1 = apps_v1
        self.policy_v1 = policy_v1
    
    def get_pods_on_node(self, node_name: str) -> List[Dict]:
        """Get all pods running on a specific node"""
        try:
            pods = self.core_v1.list_pod_for_all_namespaces(
                field_selector=f"spec.nodeName={node_name}"
            )
            return [self._pod_to_dict(pod) for pod in pods.items]
        except ApiException as e:
            logger.error(f"Failed to get pods on node {node_name}: {e}")
            return []
    
    def check_pdb_compliance(self, pods: List[Dict]) -> bool:
        """Check if pod migration complies with Pod Disruption Budgets"""
        try:
            for pod in pods:
                namespace = pod['metadata']['namespace']
                pod_labels = pod['metadata'].get('labels', {})
                
                # Get all PDBs in the namespace
                pdbs = self.policy_v1.list_namespaced_pod_disruption_budget(namespace)
                
                for pdb in pdbs.items:
                    pdb_selector = pdb.spec.selector.match_labels
                    
                    # Check if this pod matches the PDB selector
                    if all(pod_labels.get(k) == v for k, v in pdb_selector.items()):
                        # Check if eviction would violate PDB
                        current_healthy = pdb.status.current_healthy or 0
                        min_available = getattr(pdb.spec, 'min_available', None)
                        max_unavailable = getattr(pdb.spec, 'max_unavailable', None)
                        
                        if min_available is not None and current_healthy - 1 < min_available:
                            logger.warning(f"PDB violation: {pdb.metadata.name} requires min {min_available} available")
                            return False
                        
                        if max_unavailable is not None:
                            # Calculate current unavailable
                            desired = pdb.status.desired_healthy or 0
                            current_unavailable = desired - current_healthy
                            if current_unavailable + 1 > max_unavailable:
                                logger.warning(f"PDB violation: {pdb.metadata.name} allows max {max_unavailable} unavailable")
                                return False
            
            return True
            
        except ApiException as e:
            logger.error(f"Failed to check PDB compliance: {e}")
            return False
    
    def evict_pod(self, pod_name: str, namespace: str) -> bool:
        """Safely evict a pod"""
        try:
            # Using eviction API for safe pod removal
            eviction_body = {
                "apiVersion": "policy/v1",
                "kind": "Eviction",
                "metadata": {
                    "name": pod_name,
                    "namespace": namespace
                }
            }
            
            self.core_v1.create_namespaced_pod_eviction(
                name=pod_name,
                namespace=namespace,
                body=eviction_body
            )
            
            logger.info(f"Successfully evicted pod {namespace}/{pod_name}")
            return True
            
        except ApiException as e:
            logger.error(f"Failed to evict pod {namespace}/{pod_name}: {e}")
            return False
    
    def _pod_to_dict(self, pod) -> Dict:
        """Convert pod object to dictionary"""
        return {
            'metadata': {
                'name': pod.metadata.name,
                'namespace': pod.metadata.namespace,
                'labels': pod.metadata.labels or {}
            },
            'spec': {
                'nodeName': pod.spec.node_name
            },
            'status': {
                'phase': pod.status.phase
            }
        }