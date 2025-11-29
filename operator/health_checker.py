import logging
import time
from typing import List

from kubernetes.client.rest import ApiException

logger = logging.getLogger(__name__)

class HealthChecker:
    def __init__(self, core_v1):
        self.core_v1 = core_v1
    
    def wait_for_pod_ready(self, pod_name: str, namespace: str, timeout: int = 300) -> bool:
        """Wait for a pod to become ready"""
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            try:
                pod = self.core_v1.read_namespaced_pod(pod_name, namespace)
                
                # Check if pod is running and ready
                if pod.status.phase == "Running":
                    # Check readiness conditions
                    if all(condition.ready for condition in pod.status.conditions or [] 
                           if condition.type == "Ready"):
                        logger.info(f"Pod {namespace}/{pod_name} is ready")
                        return True
                
                # Check if pod failed
                if pod.status.phase == "Failed":
                    logger.error(f"Pod {namespace}/{pod_name} failed")
                    return False
                
                logger.debug(f"Waiting for pod {namespace}/{pod_name} to be ready...")
                time.sleep(5)
                
            except ApiException as e:
                if e.status == 404:
                    # Pod might not exist yet (being rescheduled)
                    logger.debug(f"Pod {namespace}/{pod_name} not found yet, waiting...")
                    time.sleep(5)
                else:
                    logger.error(f"Error checking pod {namespace}/{pod_name}: {e}")
                    return False
        
        logger.error(f"Timeout waiting for pod {namespace}/{pod_name} to be ready")
        return False
    
    def check_application_health(self, pods: List[dict]) -> bool:
        """Check overall application health"""
        # This would implement application-specific health checks
        # For example, checking endpoints, database connections, etc.
        
        healthy_pods = 0
        for pod in pods:
            if self._is_pod_healthy(pod):
                healthy_pods += 1
        
        # Consider application healthy if majority of pods are healthy
        return healthy_pods >= len(pods) * 0.8  # 80% healthy
    
    def _is_pod_healthy(self, pod: dict) -> bool:
        """Check if a specific pod is healthy"""
        # Implement pod-specific health checks
        # This could include:
        # - Liveness probe checks
        # - Readiness probe checks  
        # - Custom application health endpoints
        # - Resource usage monitoring
        
        try:
            pod_obj = self.core_v1.read_namespaced_pod(
                pod['metadata']['name'],
                pod['metadata']['namespace']
            )
            
            # Basic health check - pod is running and ready
            return (pod_obj.status.phase == "Running" and
                   any(condition.ready for condition in pod_obj.status.conditions 
                       if condition.type == "Ready"))
                       
        except ApiException:
            return False