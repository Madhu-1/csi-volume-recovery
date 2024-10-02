package kubernetes

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/retry"
	v1alpha1 "k8s.io/kubelet/pkg/apis/stats/v1alpha1"
)

type Client interface {
	GetMetrics(context.Context) (*v1alpha1.Summary, error)
	GetPVC(ctx context.Context, pvcName, namespace string) (*v1.PersistentVolumeClaim, error)
	GetPV(ctx context.Context, pvName string) (*v1.PersistentVolume, error)
	findTopOwner(namespace string, ownerRefs []metav1.OwnerReference) (string, string, error)
	ScaleOwner(namespace string, podName string, replicaCount int32) error
	RestartPod(ctx context.Context, namespace, podName string) error
}
type client struct {
	*kubernetes.Clientset
	nodeName string
	timeout  time.Duration
}

var _ Client = &client{}

func NewClient(kubeconfigpath, nodeName string) (Client, error) {
	var config *rest.Config
	var err error
	if kubeconfigpath != "" {
		if _, err = os.Stat(kubeconfigpath); err != nil {
			return nil, fmt.Errorf("error fetching kubeconfig path: %s %w", kubeconfigpath, err)
		}
		config, err = clientcmd.BuildConfigFromFlags("", kubeconfigpath)
		if err != nil {
			return nil, fmt.Errorf("failed to build config from kubeconfig: %w", err)
		}
	} else {
		config, err = rest.InClusterConfig()
		if err != nil {
			return nil, fmt.Errorf("failed to build in cluster config: %w", err)
		}
	}

	// creates the clientset
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create clientset: %w", err)
	}

	return &client{
		clientset,
		nodeName,
		2 * time.Minute,
	}, nil
}

func (c *client) GetMetrics(ctx context.Context) (*v1alpha1.Summary, error) {
	url := fmt.Sprintf("/api/v1/nodes/%s/proxy/stats/summary", c.nodeName)
	summary := &v1alpha1.Summary{}
	result, err := c.Clientset.NodeV1().RESTClient().Get().AbsPath(url).DoRaw(ctx)
	if err != nil {
		return summary, err
	}
	if err := json.Unmarshal(result, summary); err != nil {
		return summary, err
	}

	return summary, nil
}

func (c *client) GetPVC(ctx context.Context, pvcName, namespace string) (*v1.PersistentVolumeClaim, error) {
	pvc, err := c.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvcName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get PVC %s in namespace %s: %w", pvcName, namespace, err)
	}
	return pvc, nil
}

func (c *client) GetPV(ctx context.Context, pvName string) (*v1.PersistentVolume, error) {
	pv, err := c.CoreV1().PersistentVolumes().Get(ctx, pvName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get PV %s: %w", pvName, err)
	}
	return pv, nil
}

func (c *client) RestartPod(ctx context.Context, namespace, podName string) error {
	// check if there a owner for the pod , if there is a owner then delete the owner and let the owner recreate the pod
	// if not return error saying no owner exists to take care of the pod
	pod, err := c.CoreV1().Pods(namespace).Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to get pod %s in namespace %s: %w", podName, namespace, err)
	}
	ownerName, _, err := c.findTopOwner(namespace, pod.OwnerReferences)
	if err != nil {
		return fmt.Errorf("failed to find top owner for pod %s in namespace %s: %w", podName, namespace, err)
	}
	if ownerName == "" {
		return fmt.Errorf("no owner found for pod %s in namespace %s", podName, namespace)
	}
	err = c.CoreV1().Pods(namespace).Delete(ctx, podName, metav1.DeleteOptions{})
	if err != nil {
		return fmt.Errorf("failed to delete pod %s in namespace %s: %w", podName, namespace, err)
	}
	return nil
}

// Function to find the top owner recursively
func (c *client) findTopOwner(namespace string, ownerRefs []metav1.OwnerReference) (string, string, error) {
	if len(ownerRefs) == 0 {
		return "", "", nil
	}

	ownerRef := ownerRefs[0] // Assume first owner for simplicity

	switch ownerRef.Kind {
	case "ReplicaSet":
		rs, err := c.AppsV1().ReplicaSets(namespace).Get(context.TODO(), ownerRef.Name, metav1.GetOptions{})
		if err != nil {
			return "", "", err
		}
		return c.findTopOwner(namespace, rs.OwnerReferences)

	case "StatefulSet":
		// StatefulSet is typically a top-level owner
		return ownerRef.Name, "StatefulSet", nil

	case "Deployment":
		// Deployment is a top-level owner
		return ownerRef.Name, "Deployment", nil

	case "DaemonSet":
		// DaemonSet is typically a top owner as well
		return ownerRef.Name, "DaemonSet", nil

	default:
		// If it's not a known controller, return this owner as the top one
		return ownerRef.Name, ownerRef.Kind, nil
	}
}

// Function to scale the owner and wait for replicas
func (c *client) ScaleOwner(namespace string, podName string, replicaCount int32) error {
	pod, err := c.CoreV1().Pods(namespace).Get(context.Background(), podName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to get pod %s in namespace %s: %w", podName, namespace, err)
	}
	ownerRefs := pod.OwnerReferences
	ownerName, kind, err := c.findTopOwner(namespace, ownerRefs)
	if err != nil {
		return fmt.Errorf("failed to find top owner: %w", err)
	}

	// Get the scaling client for the appropriate type (Deployment, StatefulSet, etc.)
	switch kind {
	case "Deployment":
		return c.scaleDeployment(ownerName, namespace, replicaCount)

	case "StatefulSet":
		return c.scaleStateFulSet(ownerName, namespace, replicaCount)
	}

	return fmt.Errorf("unsupported owner kind: %s", kind)
}

// Scale deployment function
func (c *client) scaleDeployment(name, namespace string, count int32) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		// Get the current deployment
		deployment, err := c.AppsV1().Deployments(namespace).Get(context.TODO(), name, metav1.GetOptions{})
		if err != nil {
			return err
		}

		// Save the original replica count before scaling
		originalReplicas := deployment.Spec.Replicas
		if count != 0 {
			originalReplicas = &count
		}

		deployment.Spec.Replicas = int32Ptr(int32(count))
		if count == 0 {
			_, err = c.AppsV1().Deployments(namespace).Update(context.TODO(), deployment, metav1.UpdateOptions{})
			if err != nil {
				return err
			}
			err = c.waitForReplicasToBeZero(namespace, name, "Deployment")
			if err != nil {
				// If there was an error, revert the changes
				deployment.Spec.Replicas = originalReplicas
				_, err = c.AppsV1().Deployments(namespace).Update(context.TODO(), deployment, metav1.UpdateOptions{})
				if err != nil {
					return fmt.Errorf("failed to revert changes: %w", err)
				}
				return fmt.Errorf("failed to scale down the deployment: %w", err)
			}
		}
		deployment.Spec.Replicas = originalReplicas
		_, err = c.AppsV1().Deployments(namespace).Update(context.TODO(), deployment, metav1.UpdateOptions{})
		if err != nil {
			return fmt.Errorf("failed to revert back the replicas in deployment: %w", err)
		}
		return nil
	})
}

// Scale deployment function
func (c *client) scaleStateFulSet(name, namespace string, count int32) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		// Get the current deployment
		sts, err := c.AppsV1().StatefulSets(namespace).Get(context.TODO(), name, metav1.GetOptions{})
		if err != nil {
			return err
		}

		// Save the original replica count before scaling
		originalReplicas := sts.Spec.Replicas
		if count != 0 {
			originalReplicas = &count
		}

		sts.Spec.Replicas = int32Ptr(int32(count))

		if count == 0 {
			_, err = c.AppsV1().StatefulSets(namespace).Update(context.TODO(), sts, metav1.UpdateOptions{})
			if err != nil {
				return err
			}
			err = c.waitForReplicasToBeZero(namespace, name, "StatefulSets")
			if err != nil {
				// If there was an error, revert the changes
				sts.Spec.Replicas = originalReplicas
				_, err = c.AppsV1().StatefulSets(namespace).Update(context.TODO(), sts, metav1.UpdateOptions{})
				if err != nil {
					return fmt.Errorf("failed to revert changes: %w", err)
				}
				return fmt.Errorf("failed to scale down the StatefulSets: %w", err)
			}
		}
		sts.Spec.Replicas = originalReplicas
		_, err = c.AppsV1().StatefulSets(namespace).Update(context.TODO(), sts, metav1.UpdateOptions{})
		if err != nil {
			return fmt.Errorf("failed to revert back the replicas in StatefulSets: %w", err)
		}
		return nil
	})
}

// Wait until the replicas of the deployment or statefulset are 0
func (c *client) waitForReplicasToBeZero(namespace, ownerName, kind string) error {
	timeout := c.timeout
	ctx := context.TODO()

	return wait.PollUntilContextTimeout(ctx, 2*time.Second, timeout, true, func(ctx context.Context) (bool, error) {
		ready, err := c.checkReplicas(namespace, ownerName, kind)
		if err != nil {
			return false, err
		}
		if ready {
			return true, nil
		}
		return false, nil
	})
}

// Check the number of replicas of the owner (Deployment/StatefulSet)
func (c *client) checkReplicas(namespace, ownerName, kind string) (bool, error) {
	switch kind {
	case "Deployment":
		deployment, err := c.AppsV1().Deployments(namespace).Get(context.TODO(), ownerName, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		return deployment.Status.Replicas == 0, nil

	case "StatefulSet":
		statefulset, err := c.AppsV1().StatefulSets(namespace).Get(context.TODO(), ownerName, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		return statefulset.Status.Replicas == 0, nil
	}
	return false, fmt.Errorf("unsupported kind: %s", kind)
}

// Helper function to get a pointer to an int32
func int32Ptr(i int32) *int32 {
	return &i
}
