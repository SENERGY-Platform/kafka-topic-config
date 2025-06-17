/*
 * Copyright 2025 InfAI (CC SES)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package pkg

import (
	"context"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	testclient "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	"time"
)

func GetKubernetesClient() (*kubernetes.Clientset, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}
	return kubernetes.NewForConfig(config)
}

func GetKubernetesTestClient() (*testclient.Clientset, error) {
	return testclient.NewClientset(), nil
}

// RestartApp restarts by pod delete because kubernetes will start new pods
func RestartApp(client kubernetes.Interface, ns string, app string) error {
	timeoutCtx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	pods, err := client.CoreV1().Pods(ns).List(timeoutCtx, metav1.ListOptions{})
	if err != nil {
		return err
	}
	for _, pod := range pods.Items {
		if pod.Namespace == ns && pod.Labels["app"] == app {
			timeoutCtx, _ := context.WithTimeout(context.Background(), 10*time.Second)
			err = client.CoreV1().Pods(pod.Namespace).Delete(timeoutCtx, pod.Name, metav1.DeleteOptions{})
			if err != nil {
				return err
			}
		}
	}
	return nil
}
