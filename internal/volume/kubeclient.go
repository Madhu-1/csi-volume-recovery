package volume

import (
	"context"
	"fmt"

	"github.com/Madhu-1/csi-volume-recovery/internal/kubernetes"
	v1 "k8s.io/api/core/v1"
)

type kubeclient struct {
	clientset kubernetes.Client
}

var _ Volume = &kubeclient{}

func NewKubeVolumeClient(clientset kubernetes.Client) Volume {
	return &kubeclient{
		clientset: clientset,
	}
}

// GetDriverName returns the driver name of the volume
func (k *kubeclient) GetDriverName(ctx context.Context, _, _ string, pvcName, namespace string) (string, error) {
	pvc, err := k.getPVC(ctx, pvcName, namespace)
	if err != nil {
		return "", err
	}
	if pvc.Annotations != nil {
		driverName := pvc.Annotations["volume.beta.kubernetes.io/storage-provisioner"]
		if driverName != "" {
			return driverName, nil
		}
		driverName = pvc.Annotations["volume.kubernetes.io/storage-provisioner"]
		if driverName != "" {
			return driverName, nil
		}
	}
	pvName := pvc.Spec.VolumeName
	pv, err := k.clientset.GetPV(ctx, pvName)
	if err != nil {
		return "", fmt.Errorf("failed to get PV %s: %w", pvName, err)
	}
	if pv.Spec.CSI == nil {
		return "", fmt.Errorf("PV %s is not a CSI volume", pvName)
	}
	return pv.Spec.CSI.Driver, nil
}

func (k *kubeclient) getPVC(ctx context.Context, pvcName, namespace string) (*v1.PersistentVolumeClaim, error) {
	pvc, err := k.clientset.GetPVC(ctx, pvcName, namespace)
	if err != nil {
		return nil, fmt.Errorf("failed to get PVC %s in namespace %s: %w", pvcName, namespace, err)
	}
	return pvc, nil
}
