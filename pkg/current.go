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
	"fmt"
	"github.com/SENERGY-Platform/kafka-topic-config/pkg/configuration"
	"github.com/goccy/go-yaml"
	"github.com/segmentio/kafka-go"
	"os"
	"regexp"
	"slices"
	"strings"
)

func LogCurrentState(config configuration.Config, client *kafka.Client, partitions []kafka.Partition) (err error) {
	currentState, err := GetCurrentState(config, client, partitions)
	output, err := yaml.Marshal(currentState)
	if err != nil {
		return err
	}
	fmt.Printf("================== CURRENT STATE ==================\n%s\n===================================================\n", output)
	if config.LogCurrentStateToFile != "" && config.LogCurrentStateToFile != "-" {
		err = os.WriteFile(config.LogCurrentStateToFile, output, 0644)
		if err != nil {
			return err
		}
	}
	return nil
}

func GetCurrentState(config configuration.Config, client *kafka.Client, partitions []kafka.Partition) (result configuration.TopicConfigs, err error) {
	topics := []string{}

	var match *regexp.Regexp
	var notMatch *regexp.Regexp

	if config.LogCurrentStateIfTopicDoesNotMatchRegex != "" && config.LogCurrentStateIfTopicDoesNotMatchRegex != "-" {
		notMatch, err = regexp.Compile(config.LogCurrentStateIfTopicDoesNotMatchRegex)
		if err != nil {
			return result, err
		}
	}
	if config.LogCurrentStateIfTopicMatchesRegex != "" && config.LogCurrentStateIfTopicMatchesRegex != "-" {
		match, err = regexp.Compile(config.LogCurrentStateIfTopicMatchesRegex)
		if err != nil {
			return result, err
		}
	}

	for _, partition := range partitions {
		topic := partition.Topic
		if match != nil && !match.MatchString(topic) {
			continue
		}
		if notMatch != nil && notMatch.MatchString(topic) {
			continue
		}
		if !slices.Contains(topics, topic) {
			topics = append(topics, topic)
		}
	}

	configFilter := []kafka.DescribeConfigRequestResource{}
	for _, topic := range topics {
		configFilter = append(configFilter, kafka.DescribeConfigRequestResource{
			ResourceType: kafka.ResourceTypeTopic,
			ResourceName: topic,
		})
	}
	descr, err := client.DescribeConfigs(context.Background(), &kafka.DescribeConfigsRequest{Resources: configFilter})
	if err != nil {
		return result, err
	}

	result.Version = 1
	for _, topic := range topics {
		topicDescr, err := GetCurrentTopicDescription(partitions, descr, topic)
		if err != nil {
			return result, err
		}
		result.Topics = append(result.Topics, topicDescr)
	}
	slices.SortFunc(result.Topics, func(a, b configuration.TopicConfig) int {
		return strings.Compare(a.Name, b.Name)
	})
	return result, nil
}

func GetCurrentTopicDescription(partitions []kafka.Partition, descr *kafka.DescribeConfigsResponse, topic string) (result configuration.TopicConfig, err error) {
	result.Name = topic
	result.Config = map[string]string{}
	for _, entry := range GetTopicConfig(descr, topic) {
		result.Config[entry.ConfigName] = entry.ConfigValue
	}
	topicPartitions := []kafka.Partition{}
	currentReplication := 0
	for _, partition := range partitions {
		if partition.Topic == topic {
			topicPartitions = append(topicPartitions, partition)
			replicas := len(partition.Replicas)
			if currentReplication < replicas {
				currentReplication = replicas
			}
		}
	}
	currentTopicPartitionCount := len(topicPartitions)

	result.Partitions = currentTopicPartitionCount
	result.Replicas = currentReplication

	return result, nil
}

func GetTopicConfig(descr *kafka.DescribeConfigsResponse, topic string) (result []kafka.DescribeConfigResponseConfigEntry) {
	for _, resource := range descr.Resources {
		if resource.ResourceName == topic {
			return FilterKafkaTopicConfigSources(resource.ConfigEntries)
		}
	}
	return nil
}

func FilterKafkaTopicConfigSources(list []kafka.DescribeConfigResponseConfigEntry) []kafka.DescribeConfigResponseConfigEntry {
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
