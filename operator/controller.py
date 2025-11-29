import logging
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from kubernetes import client, watch
from kubernetes.client.rest import ApiException

from operator.node_manager import NodeManager
from operator.pod_manager import PodManager
from operator.health_checker import HealthChecker

logger = logging.getLogger(__name__)

class NodeRefreshController:
    def __init__(self, namespace: str):
        self.namespace = namespace
        self.custom_api = client.CustomObjectsApi()
        self.core_v1 = client.CoreV1Api()
        self.apps_v1 = client.AppsV1Api()
        self.policy_v1 = client.PolicyV1Api()
        
        self.node_manager = NodeManager(self.core_v1)
        self.pod_manager = PodManager(self.core_v1, self.apps_v1, self.policy_v1)
        self.health_checker = HealthChecker(self.core_v1)
        
        # Track ongoing operations
        self.ongoing_operations: Dict[str, dict] = {}
    
    def reconcile_all(self):
        """Reconcile all NodeRefresh custom resources"""
        try:
            # List all NodeRefresh objects across all namespaces
            noderefreshes = self.custom_api.list_cluster_custom_object(
                group="operations.example.com",
                version="v1alpha1",
                plural="noderefreshes"
            )
            
            for nr in noderefreshes.get('items', []):
                self.reconcile(nr)
                
        except ApiException as e:
            logger.error(f"Failed to list NodeRefresh objects: {e}")
    
    def reconcile(self, noderefresh: dict):
        """Reconcile a single NodeRefresh resource"""
        name = noderefresh['metadata']['name']
        namespace = noderefresh['metadata']['namespace']
        
        logger.info(f"Reconciling NodeRefresh {namespace}/{name}")
        
        # Get current status
        current_phase = noderefresh.get('status', {}).get('phase', 'Pending')
        
        if current_phase == 'Completed':
            # Check if it's time for the next refresh cycle
            if self._should_restart_cycle(noderefresh):
                self._update_status(name, namespace, {
                    'phase': 'Pending',
                    'message': 'Starting new refresh cycle',
                    'startTime': datetime.utcnow().isoformat() + 'Z',
                    'completionTime': None,
                    'processedNodes': [],
                    'failedNodes': []
                })
            return
        
        if current_phase == 'Failed':
            # Implement retry logic
            if self._should_retry(noderefresh):
                self._update_status(name, namespace, {
                    'phase': 'Pending',
                    'message': 'Retrying failed operation'
                })
            return
        
        # Start or continue the refresh operation
        if current_phase == 'Pending':
            self._start_refresh_operation(name, namespace, noderefresh)
        elif current_phase == 'Running':
            self._continue_refresh_operation(name, namespace, noderefresh)
    
    def _should_restart_cycle(self, noderefresh: dict) -> bool:
        """Check if it's time to restart the refresh cycle (every 3 days)"""
        spec = noderefresh.get('spec', {})
        schedule = spec.get('schedule', {})
        
        if not schedule.get('enabled', False):
            return False
        
        interval_days = schedule.get('intervalDays', 3)
        completion_time = noderefresh.get('status', {}).get('completionTime')
        
        if not completion_time:
            return True
        
        try:
            completion_dt = datetime.fromisoformat(completion_time.replace('Z', '+00:00'))
            next_refresh = completion_dt + timedelta(days=interval_days)
            return datetime.utcnow() >= next_refresh
        except ValueError:
            return True
    
    def _should_retry(self, noderefresh: dict) -> bool:
        """Determine if a failed operation should be retried"""
        status = noderefresh.get('status', {})
        failed_time = status.get('completionTime')
        
        if not failed_time:
            return True
        
        try:
            failed_dt = datetime.fromisoformat(failed_time.replace('Z', '+00:00'))
            # Retry after 1 hour
            return datetime.utcnow() >= failed_dt + timedelta(hours=1)
        except ValueError:
            return True
    
    def _start_refresh_operation(self, name: str, namespace: str, noderefresh: dict):
        """Start a new node refresh operation"""
        logger.info(f"Starting refresh operation for {namespace}/{name}")
        
        spec = noderefresh['spec']
        target_selector = spec['targetNodes']['selector']
        max_concurrent = spec['targetNodes'].get('maxConcurrentNodes', 1)
        
        # Find target nodes
        target_nodes = self.node_manager.find_nodes_by_selector(target_selector)
        
        if not target_nodes:
            self._update_status(name, namespace, {
                'phase': 'Failed',
                'message': 'No nodes found matching selector',
                'completionTime': datetime.utcnow().isoformat() + 'Z'
            })
            return
        
        # Initialize status
        self._update_status(name, namespace, {
            'phase': 'Running',
            'currentNodes': target_nodes[:max_concurrent],
            'processedNodes': [],
            'failedNodes': [],
            'startTime': datetime.utcnow().isoformat() + 'Z',
            'message': f'Starting refresh of {len(target_nodes)} nodes'
        })
    
    def _continue_refresh_operation(self, name: str, namespace: str, noderefresh: dict):
        """Continue an ongoing refresh operation"""
        status = noderefresh.get('status', {})
        current_nodes = status.get('currentNodes', [])
        processed_nodes = status.get('processedNodes', [])
        failed_nodes = status.get('failedNodes', [])
        
        spec = noderefresh['spec']
        max_concurrent = spec['targetNodes'].get('maxConcurrentNodes', 1)
        target_selector = spec['targetNodes']['selector']
        
        # Refresh current batch of nodes
        for node_name in current_nodes[:]:  # Copy list for safe iteration
            if self._refresh_node(name, namespace, node_name, spec):
                processed_nodes.append(node_name)
                current_nodes.remove(node_name)
            else:
                failed_nodes.append({
                    'nodeName': node_name,
                    'reason': 'Node refresh failed',
                    'timestamp': datetime.utcnow().isoformat() + 'Z'
                })
                current_nodes.remove(node_name)
        
        # Add new nodes to current batch if we have capacity
        all_target_nodes = self.node_manager.find_nodes_by_selector(target_selector)
        remaining_nodes = [n for n in all_target_nodes 
                         if n not in processed_nodes and n not in [fn['nodeName'] for fn in failed_nodes]]
        
        while current_nodes and len(current_nodes) < max_concurrent and remaining_nodes:
            next_node = remaining_nodes.pop(0)
            current_nodes.append(next_node)
        
        # Update status
        new_status = {
            'currentNodes': current_nodes,
            'processedNodes': processed_nodes,
            'failedNodes': failed_nodes
        }
        
        # Check if operation is complete
        if not current_nodes and not remaining_nodes:
            new_status.update({
                'phase': 'Completed',
                'completionTime': datetime.utcnow().isoformat() + 'Z',
                'message': f'Refresh completed. Processed: {len(processed_nodes)}, Failed: {len(failed_nodes)}'
            })
        else:
            new_status['message'] = f'Processing {len(current_nodes)} nodes, {len(remaining_nodes)} remaining'
        
        self._update_status(name, namespace, new_status)
    
    def _refresh_node(self, name: str, namespace: str, node_name: str, spec: dict) -> bool:
        """Refresh a single node with zero downtime"""
        logger.info(f"Refreshing node {node_name}")
        
        max_pods_to_move = spec['podManagement'].get('maxPodsToMove', 3)
        min_healthy_pods = spec['podManagement'].get('minHealthyPods', 2)
        drain_timeout = spec['podManagement'].get('drainTimeout', 600)
        readiness_timeout = spec['healthChecks'].get('readinessTimeout', 300)
        
        try:
            # Step 1: Check if node is ready for refresh
            if not self.node_manager.is_node_ready_for_refresh(node_name):
                logger.warning(f"Node {node_name} is not ready for refresh")
                return False
            
            # Step 2: Get pods running on the node
            pods_on_node = self.pod_manager.get_pods_on_node(node_name)
            
            if not pods_on_node:
                logger.info(f"No pods found on node {node_name}, skipping")
                return True
            
            # Step 3: Check Pod Disruption Budgets
            if not self.pod_manager.check_pdb_compliance(pods_on_node):
                logger.warning(f"PDB check failed for node {node_name}")
                return False
            
            # Step 4: Provision replacement node (in real scenario, this would trigger cloud provider API)
            replacement_node = self.node_manager.provision_replacement_node(node_name)
            if not replacement_node:
                logger.error(f"Failed to provision replacement node for {node_name}")
                return False
            
            logger.info(f"Replacement node {replacement_node} provisioned")
            
            # Step 5: Safely migrate pods
            successful_migrations = 0
            failed_migrations = 0
            
            for pod in pods_on_node[:max_pods_to_move]:
                if self._migrate_pod(pod, node_name, replacement_node, readiness_timeout):
                    successful_migrations += 1
                else:
                    failed_migrations += 1
                    # If too many failures, abort node refresh
                    if failed_migrations > (len(pods_on_node) - min_healthy_pods):
                        logger.error(f"Too many pod migration failures on node {node_name}")
                        # Rollback: move pods back to original node
                        self._rollback_migrations(pods_on_node, node_name)
                        return False
            
            # Step 6: Drain the original node
            if successful_migrations >= min_healthy_pods:
                if self.node_manager.safely_drain_node(node_name, drain_timeout):
                    logger.info(f"Successfully refreshed node {node_name}")
                    return True
                else:
                    logger.error(f"Failed to drain node {node_name}")
            
            return False
            
        except Exception as e:
            logger.error(f"Error refreshing node {node_name}: {e}")
            return False
    
    def _migrate_pod(self, pod: dict, source_node: str, target_node: str, timeout: int) -> bool:
        """Migrate a single pod to a new node"""
        pod_name = pod['metadata']['name']
        pod_namespace = pod['metadata']['namespace']
        
        logger.info(f"Migrating pod {pod_namespace}/{pod_name} from {source_node} to {target_node}")
        
        try:
            # Step 1: Cordon the source node to prevent new pods
            self.node_manager.cordon_node(source_node)
            
            # Step 2: Create pod on target node (in real scenario, this would be handled by deployment controllers)
            # For demonstration, we'll delete and let the controller recreate on new node
            if self.pod_manager.evict_pod(pod_name, pod_namespace):
                # Wait for pod to be rescheduled and healthy
                return self.health_checker.wait_for_pod_ready(pod_name, pod_namespace, timeout)
            
            return False
            
        except Exception as e:
            logger.error(f"Failed to migrate pod {pod_namespace}/{pod_name}: {e}")
            return False
    
    def _rollback_migrations(self, pods: List[dict], original_node: str):
        """Rollback pod migrations in case of failure"""
        logger.warning(f"Rolling back pod migrations to node {original_node}")
        
        for pod in pods:
            # In a real scenario, you would have backup of pod specs
            # For this example, we just log the rollback
            logger.info(f"Would rollback pod {pod['metadata']['name']} to {original_node}")
    
    def _update_status(self, name: str, namespace: str, status_updates: dict):
        """Update the status of a NodeRefresh resource"""
        try:
            # Get current resource
            current = self.custom_api.get_namespaced_custom_object(
                group="operations.example.com",
                version="v1alpha1",
                namespace=namespace,
                plural="noderefreshes",
                name=name
            )
            
            # Update status
            if 'status' not in current:
                current['status'] = {}
            
            current['status'].update(status_updates)
            
            # Apply update
            self.custom_api.replace_namespaced_custom_object(
                group="operations.example.com",
                version="v1alpha1",
                namespace=namespace,
                plural="noderefreshes",
                name=name,
                body=current
            )
            
            logger.debug(f"Updated status for {namespace}/{name}")
            
        except ApiException as e:
            logger.error(f"Failed to update status for {namespace}/{name}: {e}")