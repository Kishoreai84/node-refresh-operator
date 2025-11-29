#!/usr/bin/env python3

import logging
import logging.config
import signal
import sys
import time
from threading import Event

from kubernetes import client, config, watch

from operator.controller import NodeRefreshController
from operator.crd import ensure_crd_exists

# Configure logging
logging.config.fileConfig('config/logging.conf')
logger = logging.getLogger(__name__)

class Operator:
    def __init__(self, namespace=None):
        self.namespace = namespace or "node-refresh-operator"
        self.shutdown_event = Event()
        
        # Load kubeconfig
        try:
            config.load_incluster_config()  # For running in cluster
        except config.ConfigException:
            try:
                config.load_kube_config()  # For local development
            except config.ConfigException:
                raise RuntimeError("Could not load kubeconfig")
        
        self.v1 = client.CoreV1Api()
        self.custom_api = client.CustomObjectsApi()
        self.controller = NodeRefreshController(self.namespace)
        
    def signal_handler(self, signum, frame):
        logger.info("Received shutdown signal, gracefully shutting down...")
        self.shutdown_event.set()
    
    def run(self):
        """Main operator loop"""
        logger.info("Starting Node Refresh Operator")
        
        # Set up signal handlers
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        
        # Ensure CRD exists
        try:
            ensure_crd_exists()
            logger.info("CRD verified/created successfully")
        except Exception as e:
            logger.error(f"Failed to ensure CRD exists: {e}")
            sys.exit(1)
        
        # Main reconciliation loop
        while not self.shutdown_event.is_set():
            try:
                self.controller.reconcile_all()
                time.sleep(30)  # Check every 30 seconds
            except Exception as e:
                logger.error(f"Error in reconciliation loop: {e}")
                time.sleep(60)  # Wait longer on error
        
        logger.info("Node Refresh Operator stopped gracefully")

if __name__ == "__main__":
    operator = Operator()
    operator.run()