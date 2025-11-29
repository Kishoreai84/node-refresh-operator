import logging
import time
from typing import List, Dict, Optional

from kubernetes.client.rest import ApiException

logger = logging.getLogger(__name__)

class NodeManager:
    def __init__(self, core_v1):
        self.core_v1 = core_v1
    
    def find_nodes_by_selector(self, selector: Dict[str, str]) -> List[str]:
        """Find nodes matching the given label selector"""
        try:
            label_selector = ",".join([f"{k}={v}" for k, v in selector.items()])
            nodes = self.core_v1.list_node(label_selector=label_selector)
            return [node.metadata.name for node in nodes.items]
        except ApiException as e:
            logger.error(f"Failed to find nodes with selector {selector}: {e}")
            return []
    
    def is_node_ready_for_refresh(self, node_name: str) -> bool:
        """Check if a node is ready to be refreshed"""
        try:
            node = self.core_v1.read_node(node_name)
            
            # Check node conditions
            for condition in node.status.conditions:
                if condition.type == "Ready" and condition.status != "True":
                    logger.warning(f"Node {node_name} is not ready")
                    return False
            
            # Check if node is already cordoned
            if node.spec.unschedulable:
                logger.warning(f"Node {node_name} is already cordoned")
                return False
            
            return True
            
        except ApiException as e:
            logger.error(f"Failed to check node {node_name}: {e}")
            return False
    
    def provision_replacement_node(self, original_node: str) -> Optional[str]:
        """Provision a replacement node (simulated for this example)"""
        # In a real implementation, this would call cloud provider APIs
        # to provision a new node with similar characteristics
        
        logger.info(f"Simulating provisioning of replacement node for {original_node}")
        
        try:
            # Get original node info to replicate
            original_node_info = self.core_v1.read_node(original_node)
            
            # Simulate finding a suitable replacement node
            # In reality, you'd look for a node with capacity or provision a new one
            all_nodes = self.core_v1.list_node()
            candidate_nodes = [
                node for node in all_nodes.items 
                if (node.metadata.name != original_node and 
                    not node.spec.unschedulable and
                    self.is_node_ready_for_refresh(node.metadata.name))
            ]
            
            if candidate_nodes:
                return candidate_nodes[0].metadata.name
            
            # If no candidate found, simulate provisioning by returning a new node name
            # In reality, you'd need to integrate with your cloud provider's API
            simulated_node_name = f"replacement-for-{original_node}"
            logger.info(f"Would provision new node: {simulated_node_name}")
            return simulated_node_name
            
        except ApiException as e:
            logger.error(f"Failed to provision replacement node: {e}")
            return None
    
    def cordon_node(self, node_name: str) -> bool:
        """Cordon a node to prevent new pod scheduling"""
        try:
            body = {
                "spec": {
                    "unschedulable": True
                }
            }
            self.core_v1.patch_node(node_name, body)
            logger.info(f"Successfully cordoned node {node_name}")
            return True
        except ApiException as e:
            logger.error(f"Failed to cordon node {node_name}: {e}")
            return False
    
    def uncordon_node(self, node_name: str) -> bool:
        """Uncordon a node to allow pod scheduling"""
        try:
            body = {
                "spec": {
                    "unschedulable": False
                }
            }
            self.core_v1.patch_node(node_name, body)
            logger.info(f"Successfully uncordoned node {node_name}")
            return True
        except ApiException as e:
            logger.error(f"Failed to uncordon node {node_name}: {e}")
            return False
    
    def safely_drain_node(self, node_name: str, timeout: int = 600) -> bool:
        """Safely drain a node with timeout"""
        try:
            # Cordon the node first
            if not self.cordon_node(node_name):
                return False
            
            # Wait for pods to be evacuated (simplified)
            start_time = time.time()
            while time.time() - start_time < timeout:
                pods = self.core_v1.list_pod_for_all_namespaces(
                    field_selector=f"spec.nodeName={node_name}"
                )
                
                if not pods.items:
                    logger.info(f"All pods evacuated from node {node_name}")
                    return True
                
                logger.info(f"Waiting for {len(pods.items)} pods to be evacuated from {node_name}")
                time.sleep(30)
            
            logger.error(f"Timeout waiting for pods to evacuate from {node_name}")
            return False
            
        except ApiException as e:
            logger.error(f"Failed to drain node {node_name}: {e}")
            return False