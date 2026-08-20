package annotator

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/NVIDIA/go-nvml/pkg/nvml"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"
)

type Annotator struct {
	nvmlLib    nvml.Interface
	kubeClient kubernetes.Interface
	nodeName   string
	interval   time.Duration
	threshold  uint
}

// New 从环境变量读 NODE_NAME，构造 k8s 客户端。
func New(nvmlLib nvml.Interface, interval time.Duration, threshold uint) (*Annotator, error) {
	nodeName := os.Getenv("NODE_NAME")
	if nodeName == "" {
		return nil, fmt.Errorf("NODE_NAME environment variable not set")
	}

	// 优先用 ServiceAccount（Pod 内自动挂载），失败则回退 kubeconfig
	config, err := rest.InClusterConfig()
	if err != nil {
		config, err = clientcmd.BuildConfigFromFlags("", clientcmd.RecommendedHomeFile)
		if err != nil {
			return nil, fmt.Errorf("failed to build k8s config: %v", err)
		}
	}
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create k8s client: %v", err)
	}

	return &Annotator{
		nvmlLib:    nvmlLib,
		kubeClient: client,
		nodeName:   nodeName,
		interval:   interval,
		threshold:  threshold,
	}, nil
}

// Run 阻塞式循环，每 interval 秒更新一次节点 annotation。
func (a *Annotator) Run(stop <-chan struct{}) {
	ticker := time.NewTicker(a.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := a.updateAnnotations(); err != nil {
				klog.Errorf("failed to update GPU health annotations: %v", err)
			}
		case <-stop:
			return
		}
	}
}

func (a *Annotator) updateAnnotations() error {
	detail, total, healthy := a.checkGPUs()

	summary := fmt.Sprintf("%d/%d", healthy, total)
	detailJSON, _ := json.Marshal(detail)

	patch := map[string]interface{}{
		"metadata": map[string]interface{}{
			"annotations": map[string]string{
				"nvidia.com/gpu-health-summary": summary,
				"nvidia.com/gpu-health-detail":  string(detailJSON),
			},
		},
	}
	patchBytes, _ := json.Marshal(patch)

	_, err := a.kubeClient.CoreV1().Nodes().Patch(
		context.TODO(),
		a.nodeName,
		"application/strategic-merge-patch+json",
		patchBytes,
		metav1.PatchOptions{},
	)
	return err
}

// checkGPUs 遍历所有 NVML 可见的 GPU，按 GPU-Util 阈值判定健康状态。
func (a *Annotator) checkGPUs() (detail map[string]string, total, healthy int) {
	detail = make(map[string]string)
	count, ret := a.nvmlLib.DeviceGetCount()
	if ret != nvml.SUCCESS {
		klog.Errorf("DeviceGetCount failed: %v", nvml.ErrorString(ret))
		return detail, 0, 0
	}

	total = count
	for i := 0; i < count; i++ {
		device, ret := a.nvmlLib.DeviceGetHandleByIndex(i)
		if ret != nvml.SUCCESS {
			detail[fmt.Sprintf("%d", i)] = fmt.Sprintf("Unknown:%s", nvml.ErrorString(ret))
			continue
		}

		util, ret := device.GetUtilizationRates()
		idx := fmt.Sprintf("%d", i)
		if ret != nvml.SUCCESS {
			detail[idx] = fmt.Sprintf("Unknown:%s", nvml.ErrorString(ret))
			continue
		}

		if uint(util.Gpu) >= a.threshold {
			detail[idx] = fmt.Sprintf("Unhealthy:GPU-Util=%d%%", util.Gpu)
		} else {
			detail[idx] = "Healthy"
			healthy++
		}
	}
	return
}
