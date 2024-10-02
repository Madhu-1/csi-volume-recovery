package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"runtime"
	"strings"

	"log/slog"

	"github.com/Madhu-1/csi-volume-recovery/internal/csi"
	"github.com/Madhu-1/csi-volume-recovery/internal/kubernetes"
	"github.com/Madhu-1/csi-volume-recovery/internal/volume"
	"github.com/Madhu-1/csi-volume-recovery/pkg"
)

var conf = pkg.Config{}

func printVersion() {
	fmt.Println("Go Version:", runtime.Version())
	fmt.Println("Compiler:", runtime.Compiler)
	fmt.Printf("Platform: %s/%s\n", runtime.GOOS, runtime.GOARCH)

}

func init() {
	// common flags
	flag.StringVar(&conf.Endpoint, "endpoints", "", "comma separated list of CSI endpoints")
	flag.StringVar(&conf.KubeletPath, "kubelet-path", "/var/lib/kubelet", "path to kubelet directory")
	flag.StringVar(&conf.NodeName, "node-name", "minikube", "node name")
	flag.StringVar(&conf.KubeconfigPath, "kubeconfig", "kubeconfig", "path to kubeconfig file")

	flag.Parse()
}

func logAndExit(logger *slog.Logger, msg string, err error) {
	logger.Error(msg, "error", err)
	os.Exit(1)
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	printVersion()

	if conf.NodeName == "" {
		logAndExit(logger, "node name is required", nil)

	}
	kubeClient, err := kubernetes.NewClient(conf.KubeconfigPath, conf.NodeName)
	if err != nil {
		logAndExit(logger, "failed to create kubernetes client", err)
	}

	metrics, err := kubeClient.GetMetrics(context.Background())
	if err != nil {
		logAndExit(logger, "failed to get metrics", err)
	}
	logger.Info("metrics", "metrics", metrics)

	endpoints := strings.Split(conf.Endpoint, ",")
	if len(endpoints) == 0 {
		logAndExit(logger, "no CSI endpoints provided", nil)
	}
	drivers := make(map[string]csi.Client, len(endpoints))
	for _, endpoint := range endpoints {
		client, err := csi.NewClient(endpoint, logger)
		if err != nil {
			logAndExit(logger, "failed to create CSI client", err)
		}
		defer client.Close()
		drivername, err := client.GetDriverName(context.Background(), logger)
		if err != nil {
			logAndExit(logger, "failed to get driver name", err)
		}
		drivers[drivername] = client
	}
	for name, client := range drivers {
		healthy, err := client.IsHealthy(context.Background(), logger)
		if err != nil {
			logger.Error("failed to check if the node service is healthy", "driver", name, "error", err)
			continue
		}
		if !healthy {
			logger.Error("node service is not healthy", "driverName", name)
			continue
		}
	}

	client := volume.NewKubeVolumeClient(kubeClient)

	for i := range metrics.Pods {
		podName := metrics.Pods[i].PodRef.Name
		podUUID := metrics.Pods[i].PodRef.UID
		for j := range metrics.Pods[i].VolumeStats {
			pvcRef := metrics.Pods[i].VolumeStats[j].PVCRef
			if pvcRef == nil {
				continue
			}
			driver, err := client.GetDriverName(context.Background(), podUUID, podName, pvcRef.Name, pvcRef.Namespace)
			if err != nil {
				logger.Error("failed to get driver name", "error", err)
				continue
			}
			client, ok := drivers[driver]
			if !ok {
				logger.Info("driver not found", "driver", driver)
				continue
			}
			ok, err = client.NodeSupportsVolumeCondition(context.Background(), logger)
			if err != nil {
				logger.Error("failed to check if the node supports volume condition", "driver", driver, "error", err)
				continue
			}
			if !ok {
				logger.Info("node does not support volume condition", "driver", driver)
				continue
			}
			ok, err = client.NodeSupportsStageUnstage(context.Background(), logger)
			if err != nil {
				logger.Error("failed to check if the node supports stage unstage", "driver", driver, "error", err)
				continue
			}
			logger.Info("node supports volume condition and stage unstage", "driver", driver)
			if !ok {
				logger.Info("node does not support stage unstage", "driver", driver)
				err = kubeClient.RestartPod(context.Background(), pvcRef.Namespace, podName)
				if err != nil {
					logger.Error("failed to restart pod", "error", err)
				}
				continue
			} else {
				logger.Info("node supports stage unstage", "driver", driver)
				err = kubeClient.ScaleOwner(pvcRef.Namespace, podName, 0)
				if err != nil {
					logger.Error("failed to scale owner", "error", err)
				}
			}
		}
	}
}
