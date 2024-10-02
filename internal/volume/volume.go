package volume

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

type Volume interface {
	GetDriverName(ctx context.Context, podUUID, podName, pvcName, namespace string) (string, error)
}

type localHost struct {
	kubeletPath string
}

var _ Volume = &localHost{}

func NewLocalHost(kubeletPath string) Volume {
	return &localHost{
		kubeletPath: kubeletPath,
	}
}

func (l *localHost) GetDriverName(_ context.Context, podUUID, podName, pvcName, namespace string) (string, error) {
	pvName := "" // get the pv name
	filePath := filepath.Join(
		l.kubeletPath,
		"pods",
		podUUID,
		"volumes/kubernetes.io~csi/",
		pvName,
		"vol_data.json",
	)

	type volumeData struct {
		DriverName           string `json:"driverName"`
		PersistentVolumeName string `json:"specVolID"`
		VolumeHandle         string `json:"volumeHandle"`
	}
	vol := volumeData{}

	data, err := os.ReadFile(filePath)
	if err != nil {
		return "", err
	}

	err = json.Unmarshal(data, &vol)
	if err != nil {
		return "", fmt.Errorf("failed to unmarshal volume data %v: %w", data, err)
	}

	return vol.DriverName, nil
}
