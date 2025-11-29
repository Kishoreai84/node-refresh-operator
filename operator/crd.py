import logging

from kubernetes import client
from kubernetes.client.rest import ApiException

logger = logging.getLogger(__name__)

def ensure_crd_exists():
    """Ensure the NodeRefresh CRD exists in the cluster"""
    crd_body = {
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {
            "name": "noderefreshes.operations.example.com"
        },
        "spec": {
            "group": "operations.example.com",
            "versions": [
                {
                    "name": "v1alpha1",
                    "served": True,
                    "storage": True,
                    "schema": {
                        "openAPIV3Schema": {
                            "type": "object",
                            "properties": {
                                "spec": {
                                    "type": "object",
                                    "properties": {
                                        "targetNodes": {
                                            "type": "object",
                                            "properties": {
                                                "selector": {
                                                    "type": "object",
                                                    "additionalProperties": {
                                                        "type": "string"
                                                    }
                                                },
                                                "maxConcurrentNodes": {
                                                    "type": "integer",
                                                    "minimum": 1,
                                                    "maximum": 10,
                                                    "default": 1
                                                }
                                            }
                                        },
                                        "podManagement": {
                                            "type": "object",
                                            "properties": {
                                                "maxPodsToMove": {
                                                    "type": "integer", 
                                                    "minimum": 1,
                                                    "default": 3
                                                },
                                                "minHealthyPods": {
                                                    "type": "integer",
                                                    "minimum": 1, 
                                                    "default": 2
                                                },
                                                "drainTimeout": {
                                                    "type": "integer",
                                                    "minimum": 60,
                                                    "default": 600
                                                }
                                            }
                                        },
                                        "schedule": {
                                            "type": "object", 
                                            "properties": {
                                                "enabled": {
                                                    "type": "boolean",
                                                    "default": False
                                                },
                                                "intervalDays": {
                                                    "type": "integer",
                                                    "minimum": 1,
                                                    "default": 3
                                                }
                                            }
                                        },
                                        "healthChecks": {
                                            "type": "object",
                                            "properties": {
                                                "readinessTimeout": {
                                                    "type": "integer",
                                                    "minimum": 10,
                                                    "default": 300
                                                },
                                                "livenessChecks": {
                                                    "type": "boolean", 
                                                    "default": True
                                                }
                                            }
                                        }
                                    }
                                },
                                "status": {
                                    "type": "object",
                                    "properties": {
                                        "phase": {
                                            "type": "string",
                                            "enum": ["Pending", "Running", "Completed", "Failed"]
                                        },
                                        "currentNodes": {
                                            "type": "array",
                                            "items": {
                                                "type": "string"
                                            }
                                        },
                                        "processedNodes": {
                                            "type": "array", 
                                            "items": {
                                                "type": "string"
                                            }
                                        },
                                        "failedNodes": {
                                            "type": "array",
                                            "items": {
                                                "type": "object",
                                                "properties": {
                                                    "nodeName": {"type": "string"},
                                                    "reason": {"type": "string"},
                                                    "timestamp": {"type": "string"}
                                                }
                                            }
                                        },
                                        "startTime": {"type": "string"},
                                        "completionTime": {"type": "string"},
                                        "message": {"type": "string"}
                                    }
                                }
                            }
                        }
                    }
                }
            ],
            "scope": "Namespaced",
            "names": {
                "plural": "noderefreshes",
                "singular": "noderefresh", 
                "kind": "NodeRefresh",
                "shortNames": ["nr"]
            }
        }
    }
    
    v1 = client.ApiextensionsV1Api()
    
    try:
        # Try to get the CRD
        v1.get_custom_resource_definition("noderefreshes.operations.example.com")
        logger.info("NodeRefresh CRD already exists")
    except ApiException as e:
        if e.status == 404:
            # CRD doesn't exist, create it
            try:
                v1.create_custom_resource_definition(crd_body)
                logger.info("Successfully created NodeRefresh CRD")
            except ApiException as create_e:
                logger.error(f"Failed to create NodeRefresh CRD: {create_e}")
                raise
        else:
            logger.error(f"Error checking NodeRefresh CRD: {e}")
            raise