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
	"errors"
	"fmt"
	"github.com/SENERGY-Platform/kafka-topic-config/pkg/configuration"
	"github.com/segmentio/kafka-go"
	"k8s.io/client-go/kubernetes"
	"math/rand"
	"slices"
	"time"
)

type KafkaClient interface {
	DeleteTopics(ctx context.Context, k *kafka.DeleteTopicsRequest) (*kafka.DeleteTopicsResponse, error)
	CreateTopics(ctx context.Context, k *kafka.CreateTopicsRequest) (*kafka.CreateTopicsResponse, error)
	AlterConfigs(ctx context.Context, k *kafka.AlterConfigsRequest) (*kafka.AlterConfigsResponse, error)
	CreatePartitions(ctx context.Context, k *kafka.CreatePartitionsRequest) (*kafka.CreatePartitionsResponse, error)
	AlterPartitionReassignments(ctx context.Context, k *kafka.AlterPartitionReassignmentsRequest) (*kafka.AlterPartitionReassignmentsResponse, error)
}

func SetTopics(config configuration.Config, kubernetesClient kubernetes.Interface, topics []configuration.TopicConfig) (err error) {
	broker, partitions, err := GetBrokerInfo(config.KafkaUrl)
	if err != nil {
		return err
	}

	brokerAddr, err := BrokerToAddrList(broker)
	if err != nil {
		return err
	}

	requestLogger := &KafkaClientRequestLogger{}

	client := &kafka.Client{
		Addr: kafka.TCP(brokerAddr...),
	}

	//collect commands

	var commands Commands

	if config.LogCurrentState {
		err = LogCurrentState(config, client, partitions)
		if err != nil {
			return err
		}
	}

	for _, topic := range topics {
		temp, err := CollectCommandsForTopic(config, broker, partitions, topic)
		if err != nil {
			return err
		}
		commands.Merge(temp)
	}

	//exec commands

	if config.DryRun {
		fmt.Println("dry-run")
	}

	if config.AllowTopicDelete {
		requestLogger.DeleteTopics(&kafka.DeleteTopicsRequest{Topics: commands.deleteTopics})
		if !config.DryRun && len(commands.deleteTopics) > 0 {
			_, err = client.DeleteTopics(context.Background(), &kafka.DeleteTopicsRequest{Topics: commands.deleteTopics})
			if err != nil {
				return err
			}
			time.Sleep(5 * time.Second) //wait to allow delete to finish before creating new
		}
	}

	requestLogger.CreateTopics(&kafka.CreateTopicsRequest{Topics: commands.createTopics})
	if !config.DryRun && len(commands.createTopics) > 0 {
		_, err = client.CreateTopics(context.Background(), &kafka.CreateTopicsRequest{Topics: commands.createTopics})
		if err != nil {
			return err
		}
	}

	requestLogger.AlterConfigs(&kafka.AlterConfigsRequest{Resources: commands.alterConfig})
	if !config.DryRun && len(commands.alterConfig) > 0 {
		_, err = client.AlterConfigs(context.Background(), &kafka.AlterConfigsRequest{Resources: commands.alterConfig})
		if err != nil {
			return err
		}
	}

	requestLogger.CreatePartitions(&kafka.CreatePartitionsRequest{Topics: commands.createPartitions})
	if !config.DryRun && len(commands.createPartitions) > 0 {
		_, err = client.CreatePartitions(context.Background(), &kafka.CreatePartitionsRequest{Topics: commands.createPartitions})
		if err != nil {
			return err
		}
	}

	for _, req := range commands.alterPartitions {
		reqCp := req
		requestLogger.AlterPartitionReassignments(&reqCp)
		if !config.DryRun {
			_, err = client.AlterPartitionReassignments(context.Background(), &reqCp)
			if err != nil {
				return err
			}
		}
	}

	if kubernetesClient != nil && config.EnableKubernetesPodRestart {
		for _, topic := range commands.restarts {
			for _, pod := range topic.Restart {
				fmt.Printf("Restarting Kubernetes App ns=%v app=%v\n", pod.Namespace, pod.App)
				if !config.DryRun {
					err = RestartApp(kubernetesClient, pod.Namespace, pod.App)
				}
				if err != nil {
					return err
				}
			}
		}
	}

	return nil
}

type Commands struct {
	alterConfig      []kafka.AlterConfigRequestResource
	alterPartitions  []kafka.AlterPartitionReassignmentsRequest
	createPartitions []kafka.TopicPartitionsConfig
	createTopics     []kafka.TopicConfig
	deleteTopics     []string
	restarts         []configuration.TopicConfig
}

func (this *Commands) Merge(other Commands) {
	this.alterConfig = append(this.alterConfig, other.alterConfig...)
	this.alterPartitions = append(this.alterPartitions, other.alterPartitions...)
	this.createPartitions = append(this.createPartitions, other.createPartitions...)
	this.createTopics = append(this.createTopics, other.createTopics...)
	this.deleteTopics = append(this.deleteTopics, other.deleteTopics...)
	this.restarts = append(this.restarts, other.restarts...)
}

func CollectCommandsForTopic(config configuration.Config, broker []kafka.Broker, partitions []kafka.Partition, topic configuration.TopicConfig) (commands Commands, err error) {
	if topic.Replicas > len(broker) {
		return commands, fmt.Errorf("topic %v tries to use replication-factor %v whole only %v brokers are known", topic.Name, topic.Replicas, len(broker))
	}

	topicPartitions := []kafka.Partition{}
	currentReplication := 0
	for _, partition := range partitions {
		if partition.Topic == topic.Name {
			topicPartitions = append(topicPartitions, partition)
			replicas := len(partition.Replicas)
			if currentReplication < replicas {
				currentReplication = replicas
			}
		}
	}
	currentTopicPartitionCount := len(topicPartitions)
	isUpdate := currentTopicPartitionCount > 0
	if isUpdate {
		restartTopic := false

		//handle changed topic config
		alterConfigsResources := []kafka.AlterConfigRequestConfig{}
		for key, value := range topic.Config {
			alterConfigsResources = append(alterConfigsResources, kafka.AlterConfigRequestConfig{
				Name:  key,
				Value: value,
			})
		}
		commands.alterConfig = append(commands.alterConfig, kafka.AlterConfigRequestResource{
			ResourceType: kafka.ResourceTypeTopic,
			ResourceName: topic.Name,
			Configs:      alterConfigsResources,
		})

		//handle added partitions
		if currentTopicPartitionCount < topic.Partitions {
			commands.createPartitions = append(commands.createPartitions, kafka.TopicPartitionsConfig{
				Name:                      topic.Name,
				Count:                     int32(topic.Partitions),
				TopicPartitionAssignments: nil,
			})
			restartTopic = true
		}

		// handle removed partitions
		if currentTopicPartitionCount > topic.Partitions && config.AllowTopicDelete {
			commands.deleteTopics = append(commands.deleteTopics, topic.Name)
			topicConfigEntries := []kafka.ConfigEntry{}
			for key, value := range topic.Config {
				topicConfigEntries = append(topicConfigEntries, kafka.ConfigEntry{
					ConfigName:  key,
					ConfigValue: value,
				})
			}
			commands.createTopics = append(commands.createTopics, kafka.TopicConfig{
				Topic:             topic.Name,
				NumPartitions:     topic.Partitions,
				ReplicationFactor: topic.Replicas,
				ConfigEntries:     topicConfigEntries,
			})
			restartTopic = true
		}

		// handle changed replicas
		if currentReplication != topic.Replicas {
			assignments := []kafka.AlterPartitionReassignmentsRequestAssignment{}
			for _, partition := range topicPartitions {
				brokerIds := []int{}
				//reuse currently used assignments, limited by partition.Replicas
				for i, partitionBroker := range partition.Replicas {
					if i < topic.Replicas {
						brokerIds = append(brokerIds, partitionBroker.ID)
					}
				}
				//add random broker assignment if more are needed
				if len(brokerIds) < topic.Replicas {
					freeBrokerIds := GetFreeBrokerIds(broker, brokerIds)
					for range topic.Replicas - len(brokerIds) {
						if len(freeBrokerIds) == 0 {
							return commands, errors.New("unable to select free broker for partition assignment")
						}
						selectedId := freeBrokerIds[rand.Intn(len(freeBrokerIds))]
						brokerIds = append(brokerIds, selectedId)
						freeBrokerIds = GetFreeBrokerIds(broker, brokerIds)
					}
				}
				assignments = append(assignments, kafka.AlterPartitionReassignmentsRequestAssignment{
					Topic:       partition.Topic,
					PartitionID: partition.ID,
					BrokerIDs:   brokerIds,
				})
			}
			commands.alterPartitions = append(commands.alterPartitions, kafka.AlterPartitionReassignmentsRequest{
				Topic:       topic.Name,
				Assignments: assignments,
			})
		}
		if restartTopic {
			commands.restarts = append(commands.restarts, topic)
		}
	} else {
		//handle new topics
		topicConfigEntries := []kafka.ConfigEntry{}
		for key, value := range topic.Config {
			topicConfigEntries = append(topicConfigEntries, kafka.ConfigEntry{
				ConfigName:  key,
				ConfigValue: value,
			})
		}
		commands.createTopics = append(commands.createTopics, kafka.TopicConfig{
			Topic:             topic.Name,
			NumPartitions:     topic.Partitions,
			ReplicationFactor: topic.Replicas,
			ConfigEntries:     topicConfigEntries,
		})
	}

	return commands, nil
}

func GetFreeBrokerIds(all []kafka.Broker, used []int) (free []int) {
	for _, broker := range all {
		if !slices.Contains(used, broker.ID) {
			free = append(free, broker.ID)
		}
	}
	return free
}

type KafkaClientRequestLogger struct{}

func (this *KafkaClientRequestLogger) DeleteTopics(k *kafka.DeleteTopicsRequest) {
	fmt.Printf("Delete Topics: %+v\n", k.Topics)
}

func (this *KafkaClientRequestLogger) CreateTopics(k *kafka.CreateTopicsRequest) {
	fmt.Println("Create Topics:")
	for _, topic := range k.Topics {
		confMap := map[string]string{}
		for _, e := range topic.ConfigEntries {
			confMap[e.ConfigName] = e.ConfigValue
		}
		fmt.Printf("    name=%v partitions=%v replicas=%v config=%v\n", topic.Topic, topic.NumPartitions, topic.ReplicationFactor, confMap)
	}
}

func (this *KafkaClientRequestLogger) AlterConfigs(k *kafka.AlterConfigsRequest) {
	fmt.Println("Alter Topic Configs:")
	for _, topic := range k.Resources {
		confMap := map[string]string{}
		for _, e := range topic.Configs {
			confMap[e.Name] = e.Value
		}
		fmt.Printf("    name=%v config=%v\n", topic.ResourceName, confMap)
	}
}

func (this *KafkaClientRequestLogger) CreatePartitions(k *kafka.CreatePartitionsRequest) {
	fmt.Println("Create Partitions:")
	for _, topic := range k.Topics {
		fmt.Printf("    topic=%v partitions=%v assignments=%+v\n", topic.Name, topic.Count, topic.TopicPartitionAssignments)
	}
}

func (this *KafkaClientRequestLogger) AlterPartitionReassignments(k *kafka.AlterPartitionReassignmentsRequest) {
	fmt.Printf("Reassign Partition for Topic %v:\n", k.Topic)
	for _, assignment := range k.Assignments {
		fmt.Printf("    topic=%v partition=%v brokerIDs=%+v\n", assignment.Topic, assignment.PartitionID, assignment.BrokerIDs)
	}
}
