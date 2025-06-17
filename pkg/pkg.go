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
	"github.com/SENERGY-Platform/kafka-topic-config/pkg/configuration"
	"k8s.io/client-go/kubernetes"
)

func Run(config configuration.Config) (err error) {
	var kubeClient kubernetes.Interface
	if config.EnableKubernetesPodRestart {
		kubeClient, err = GetKubernetesClient()
		if err != nil {
			return err
		}
	}
	return RunWithKubeClient(config, kubeClient)
}

func RunWithKubeClient(config configuration.Config, kubeClient kubernetes.Interface) error {
	topics, err := configuration.LoadTopicConfigsFromYaml(config.TopicConfigLocation)
	if err != nil {
		return err
	}
	return SetTopics(config, kubeClient, topics.Topics)
}
