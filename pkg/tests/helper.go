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
	"fmt"
	"github.com/SENERGY-Platform/kafka-topic-config/pkg"
	"github.com/SENERGY-Platform/kafka-topic-config/pkg/configuration"
	"github.com/segmentio/kafka-go"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
)

// can be run with t==nil
func CheckKafkaState(t *testing.T, kafkaUrls []string, yamlLocation string) error {
	topics, err := configuration.LoadTopicConfigsFromYaml(yamlLocation)
	if err != nil {
		t.Error(err)
		return err
	}
	_, partitions, err := pkg.GetBrokerInfo(kafkaUrls[0])
	if err != nil {
		if t != nil {
			t.Error(err)
		}
		return err
	}

	slices.SortFunc(partitions, func(a, b kafka.Partition) int {
		c := strings.Compare(a.Topic, b.Topic)
		if c == 0 {
			return a.ID - b.ID
		}
		return c
	})

	client := &kafka.Client{
		Addr:    kafka.TCP(kafkaUrls...),
		Timeout: 10 * time.Second,
	}

	for _, topic := range topics.Topics {
		for i := range topic.Partitions {
			if !slices.ContainsFunc(partitions, func(e kafka.Partition) bool {
				return e.Topic == topic.Name && e.ID == i && len(e.Replicas) == topic.Replicas
			}) {
				if t != nil {
					t.Errorf("partition not found: topic=%v partition=%v replicas=%v\n", topic.Name, i, topic.Replicas)
					t.Errorf("partition-list: %#v", partitions)
				}
				return fmt.Errorf("partition not found: topic=%v partition=%v replicas=%v\n", topic.Name, i, topic.Replicas)
			}
		}

		descr, err := client.DescribeConfigs(context.Background(), &kafka.DescribeConfigsRequest{
			Resources: []kafka.DescribeConfigRequestResource{
				{
					ResourceType: kafka.ResourceTypeTopic,
					ResourceName: topic.Name,
				},
			},
		})
		if err != nil {
			if t != nil {
				t.Error(err)
			}
			return err
		}
		if len(descr.Resources) != 1 {
			if t != nil {
				t.Errorf("expected 1 resource, got %d", len(descr.Resources))
			}
			return fmt.Errorf("expected 1 resource, got %d", len(descr.Resources))
		}
		if descr.Resources[0].ResourceName != topic.Name {
			if t != nil {
				t.Errorf("expected resource name %s, got %s", topic.Name, descr.Resources[0].ResourceName)
			}
			return fmt.Errorf("expected resource name %s, got %s", topic.Name, descr.Resources[0].ResourceName)
		}
		if descr.Resources[0].Error != nil {
			if t != nil {
				t.Error(descr.Resources[0].Error)
			}
			return descr.Resources[0].Error
		}
		expected := []kafka.DescribeConfigResponseConfigEntry{}
		for k, v := range topic.Config {
			expected = append(expected, kafka.DescribeConfigResponseConfigEntry{
				ConfigName:  k,
				ConfigValue: v,
			})
		}
		slices.SortFunc(expected, func(a, b kafka.DescribeConfigResponseConfigEntry) int {
			return strings.Compare(a.ConfigName, b.ConfigName)
		})
		actual := NormalizeForTestCompare(descr.Resources[0].ConfigEntries)
		slices.SortFunc(actual, func(a, b kafka.DescribeConfigResponseConfigEntry) int {
			return strings.Compare(a.ConfigName, b.ConfigName)
		})

		if !reflect.DeepEqual(actual, expected) {
			if t != nil {
				t.Errorf("\ne=%#v\na=%#v\n", expected, actual)
			}
			return fmt.Errorf("e=%#v\na=%#v\n", expected, actual)
		}
	}

	for _, p := range partitions {
		if !slices.ContainsFunc(topics.Topics, func(topic configuration.TopicConfig) bool {
			return p.Topic == topic.Name && p.ID < topic.Partitions && len(p.Replicas) == topic.Replicas
		}) {
			if t != nil {
				t.Errorf("partition not found in config %v %v %v", p.Topic, p.ID, len(p.Replicas))
			}
			return fmt.Errorf("partition not found in config %v %v %v", p.Topic, p.ID, len(p.Replicas))
		}
	}
	return nil
}

func NormalizeForTestCompare(list []kafka.DescribeConfigResponseConfigEntry) []kafka.DescribeConfigResponseConfigEntry {
	result := []kafka.DescribeConfigResponseConfigEntry{}
	for _, entry := range list {
		//filter other sources
		if entry.ConfigSource == 1 {
			result = append(result, kafka.DescribeConfigResponseConfigEntry{
				//we check only for name and value
				ConfigName:  entry.ConfigName,
				ConfigValue: entry.ConfigValue,
			})
		}
	}
	return result
}
