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

package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/SENERGY-Platform/kafka-topic-config/pkg"
	"github.com/SENERGY-Platform/kafka-topic-config/pkg/configuration"
	"github.com/SENERGY-Platform/kafka-topic-config/pkg/tests/docker"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func Test(t *testing.T) {
	wg := &sync.WaitGroup{}
	defer wg.Wait()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	kafkaUrls, err := docker.KafkaCompose(ctx, wg)
	if err != nil {
		t.Error(err)
		return
	}

	time.Sleep(1 * time.Second)

	kubeClient, err := pkg.GetKubernetesTestClient()
	if err != nil {
		t.Error(err)
		return
	}

	for ns := range 4 {
		for app := range 4 {
			for pod := range 3 {
				_, err = kubeClient.CoreV1().Pods("ns"+strconv.Itoa(ns)).Create(context.Background(), &v1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:      fmt.Sprintf("ns%v_app%v_pod%v", ns, app, pod),
						Namespace: "ns" + strconv.Itoa(ns),
						Labels:    map[string]string{"app": "a" + strconv.Itoa(app)},
					},
				}, metav1.CreateOptions{})
				if err != nil {
					t.Error(err)
					return
				}
			}
		}
	}
	kubeClient.Fake.ClearActions()

	t.Run("init", func(t *testing.T) {
		config := configuration.Config{
			KafkaUrl:                   kafkaUrls[0],
			TopicConfigLocation:        "./resources/topic_config_init.yaml",
			AllowTopicDelete:           true,
			EnableKubernetesPodRestart: true,
		}
		err = pkg.RunWithKubeClient(config, kubeClient)
		if err != nil {
			t.Error(err)
			return
		}
	})

	time.Sleep(5 * time.Second)

	t.Run("check init kafka state", func(t *testing.T) {
		CheckKafkaState(t, kafkaUrls, "./resources/topic_config_init.yaml")
	})

	t.Run("check init kube commands", func(t *testing.T) {
		if len(kubeClient.Fake.Actions()) != 0 {
			t.Error(kubeClient.Fake.Actions())
		}
	})

	testfile := t.TempDir() + "/testfile.yaml"

	t.Run("rerun", func(t *testing.T) {
		config := configuration.Config{
			KafkaUrl:                   kafkaUrls[0],
			TopicConfigLocation:        "./resources/topic_config_init.yaml",
			AllowTopicDelete:           true,
			EnableKubernetesPodRestart: true,
			LogCurrentState:            true,
			LogCurrentStateToFile:      testfile,
		}
		err = pkg.RunWithKubeClient(config, kubeClient)
		if err != nil {
			t.Error(err)
			return
		}
	})

	t.Run("check current", func(t *testing.T) {
		actual, err := configuration.LoadTopicConfigsFromYaml(testfile)
		if err != nil {
			t.Error(err)
			return
		}
		expected, err := configuration.LoadTopicConfigsFromYaml("./resources/topic_config_init.yaml")
		if err != nil {
			t.Error(err)
			return
		}
		slices.SortFunc(expected.Topics, func(a, b configuration.TopicConfig) int {
			return strings.Compare(a.Name, b.Name)
		})
		for i, topic := range expected.Topics {
			topic.Restart = []configuration.TopicConfigRestart{}
			expected.Topics[i] = topic
		}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("\na=%#v\ne=%#v\n", actual, expected)
		}
	})

	time.Sleep(5 * time.Second)

	t.Run("check rerun kafka state", func(t *testing.T) {
		CheckKafkaState(t, kafkaUrls, "./resources/topic_config_init.yaml")
	})

	t.Run("check rerun kube commands", func(t *testing.T) {
		if len(kubeClient.Fake.Actions()) != 0 {
			t.Error(kubeClient.Fake.Actions())
		}
	})

	t.Run("update", func(t *testing.T) {
		config := configuration.Config{
			KafkaUrl:                   kafkaUrls[0],
			TopicConfigLocation:        "./resources/topic_config_update_1.yaml",
			AllowTopicDelete:           true,
			EnableKubernetesPodRestart: true,
		}
		err = pkg.RunWithKubeClient(config, kubeClient)
		if err != nil {
			t.Error(err)
			return
		}
	})

	time.Sleep(5 * time.Second)

	t.Run("check update kafka state", func(t *testing.T) {
		CheckKafkaState(t, kafkaUrls, "./resources/topic_config_update_1.yaml")
	})

	t.Run("check update kube commands", func(t *testing.T) {
		if len(kubeClient.Fake.Actions()) != 0 {
			t.Error(kubeClient.Fake.Actions())
		}
	})

	t.Run("update with kube restart", func(t *testing.T) {
		config := configuration.Config{
			KafkaUrl:                   kafkaUrls[0],
			TopicConfigLocation:        "./resources/topic_config_update_with_restart.yaml",
			AllowTopicDelete:           true,
			EnableKubernetesPodRestart: true,
		}
		err = pkg.RunWithKubeClient(config, kubeClient)
		if err != nil {
			t.Error(err)
			return
		}
	})

	time.Sleep(5 * time.Second)

	t.Run("check with kube restart kafka state", func(t *testing.T) {
		CheckKafkaState(t, kafkaUrls, "./resources/topic_config_update_with_restart.yaml")
	})

	t.Run("check with kube restart kube commands", func(t *testing.T) {
		expectedStr := `[
   {
      "Namespace":"ns3",
      "Verb":"list",
      "Resource":{
         "Group":"",
         "Version":"v1",
         "Resource":"pods"
      },
      "Subresource":"",
      "Kind":{
         "Group":"",
         "Version":"v1",
         "Kind":"Pod"
      },
      "Name":"",
      "ListRestrictions":{
         "Labels":null,
         "Fields":[
            
         ]
      },
      "ListOptions":{
         
      }
   },
   {
      "Namespace":"ns3",
      "Verb":"delete",
      "Resource":{
         "Group":"",
         "Version":"v1",
         "Resource":"pods"
      },
      "Subresource":"",
      "Name":"ns3_app3_pod0",
      "DeleteOptions":{
         
      }
   },
   {
      "Namespace":"ns3",
      "Verb":"delete",
      "Resource":{
         "Group":"",
         "Version":"v1",
         "Resource":"pods"
      },
      "Subresource":"",
      "Name":"ns3_app3_pod1",
      "DeleteOptions":{
         
      }
   },
   {
      "Namespace":"ns3",
      "Verb":"delete",
      "Resource":{
         "Group":"",
         "Version":"v1",
         "Resource":"pods"
      },
      "Subresource":"",
      "Name":"ns3_app3_pod2",
      "DeleteOptions":{
         
      }
   },
   {
      "Namespace":"ns4",
      "Verb":"list",
      "Resource":{
         "Group":"",
         "Version":"v1",
         "Resource":"pods"
      },
      "Subresource":"",
      "Kind":{
         "Group":"",
         "Version":"v1",
         "Kind":"Pod"
      },
      "Name":"",
      "ListRestrictions":{
         "Labels":null,
         "Fields":[
            
         ]
      },
      "ListOptions":{
         
      }
   }
]`
		var expectedObj interface{}
		err := json.Unmarshal([]byte(expectedStr), &expectedObj)
		if err != nil {
			t.Error(err)
			return
		}
		temp, _ := json.Marshal(kubeClient.Fake.Actions())
		var actualObj interface{}
		err = json.Unmarshal(temp, &actualObj)
		if err != nil {
			t.Error(err)
			return
		}
		if !reflect.DeepEqual(actualObj, expectedObj) {
			t.Errorf("\ne:%#v\na:%#v\n", expectedObj, actualObj)
		}
	})

	t.Run("log only", func(t *testing.T) {
		config := configuration.Config{
			KafkaUrl:              kafkaUrls[0],
			LogCurrentState:       true,
			LogCurrentStateToFile: testfile,
		}
		err = pkg.RunWithKubeClient(config, kubeClient)
		if err != nil {
			t.Error(err)
			return
		}
	})

	t.Run("check current", func(t *testing.T) {
		actual, err := configuration.LoadTopicConfigsFromYaml(testfile)
		if err != nil {
			t.Error(err)
			return
		}
		expected, err := configuration.LoadTopicConfigsFromYaml("./resources/topic_config_update_with_restart.yaml")
		if err != nil {
			t.Error(err)
			return
		}
		slices.SortFunc(expected.Topics, func(a, b configuration.TopicConfig) int {
			return strings.Compare(a.Name, b.Name)
		})
		for i, topic := range expected.Topics {
			topic.Restart = []configuration.TopicConfigRestart{}
			expected.Topics[i] = topic
		}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("\na=%#v\ne=%#v\n", actual, expected)
		}
	})

}
