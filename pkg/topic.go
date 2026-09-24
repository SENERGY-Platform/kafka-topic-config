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
	"log/slog"
	"math/rand"
	"reflect"
	"slices"
	"time"

	"github.com/SENERGY-Platform/kafka-topic-config/pkg/configuration"
	"github.com/segmentio/kafka-go"
	"k8s.io/client-go/kubernetes"
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

	adminTimeout := config.AdminRequestTimeout
	if adminTimeout <= 0 {
		adminTimeout = configuration.DefaultAdminRequestTimeout
	}

	client := &kafka.Client{
		Addr:    kafka.TCP(brokerAddr...),
		Timeout: adminTimeout,
	}

	//collect commands

	var commands Commands

	current, err := GetCurrentState(config, client, partitions)
	if err != nil {
		return err
	}

	if config.LogCurrentState {
		err = LogCurrentState(config, current)
		if err != nil {
			return err
		}
	}

	topicNameList := []string{}
	for _, topic := range topics {
		if slices.Contains(topicNameList, topic.Name) {
			config.GetLogger().Warn("found (and ignore) duplicate topic config", "topic", topic.Name)
		} else {
			temp, err := CollectCommandsForTopic(config, broker, partitions, current, topic)
			if err != nil {
				return err
			}
			commands.Merge(temp)
			topicNameList = append(topicNameList, topic.Name)
		}
	}

	temp, err := CheckDeviceTypeTopics(config, partitions)
	if err != nil {
		return err
	}
	commands.Merge(temp)

	return ExecuteCommands(config, kubernetesClient, client, requestLogger, adminTimeout, commands)
}

// ExecuteCommands sends the collected commands to the kafka cluster and, if enabled, restarts the
// affected kubernetes pods. client only needs the subset of *kafka.Client used here, so tests can
// pass a fake. A transport error aborts immediately, since no response was received to evaluate;
// a per-item error reported inside an otherwise successful response is logged and collected instead,
// so one bad topic does not stop the remaining commands from being tried and reported.
func ExecuteCommands(config configuration.Config, kubernetesClient kubernetes.Interface, client KafkaClient, requestLogger *KafkaClientRequestLogger, adminTimeout time.Duration, commands Commands) (err error) {
	logger := config.GetLogger()
	var itemErrs []error

	if config.DryRun {
		fmt.Println("dry-run")
	}

	if config.AllowTopicDelete {
		requestLogger.DeleteTopics(&kafka.DeleteTopicsRequest{Topics: commands.deleteTopics})
		if !config.DryRun && len(commands.deleteTopics) > 0 {
			deltes := Chunk(commands.deleteTopics, 100)
			for i, chunk := range deltes {
				config.GetLogger().Info("delete topics chunk", "chunk", i+1, "delete-count", len(deltes), "chunk-count", len(chunk))
				resp, err := client.DeleteTopics(context.Background(), &kafka.DeleteTopicsRequest{Topics: chunk})
				if err != nil {
					return err
				}
				itemErrs = append(itemErrs, EvalDeleteTopicsResponse(logger, resp)...)
			}
			time.Sleep(5 * time.Second) //wait to allow delete to finish before creating new
		}
	}

	requestLogger.CreateTopics(&kafka.CreateTopicsRequest{Topics: commands.createTopics})
	if !config.DryRun && len(commands.createTopics) > 0 {
		resp, err := client.CreateTopics(context.Background(), &kafka.CreateTopicsRequest{Topics: commands.createTopics})
		if err != nil {
			return err
		}
		itemErrs = append(itemErrs, EvalCreateTopicsResponse(logger, resp)...)
	}

	requestLogger.AlterConfigs(&kafka.AlterConfigsRequest{Resources: commands.alterConfig})
	if !config.DryRun && len(commands.alterConfig) > 0 {
		resp, err := client.AlterConfigs(context.Background(), &kafka.AlterConfigsRequest{Resources: commands.alterConfig})
		if err != nil {
			return err
		}
		itemErrs = append(itemErrs, EvalAlterConfigsResponse(logger, resp)...)
	}

	requestLogger.CreatePartitions(&kafka.CreatePartitionsRequest{Topics: commands.createPartitions})
	if !config.DryRun && len(commands.createPartitions) > 0 {
		resp, err := client.CreatePartitions(context.Background(), &kafka.CreatePartitionsRequest{Topics: commands.createPartitions})
		if err != nil {
			return err
		}
		itemErrs = append(itemErrs, EvalCreatePartitionsResponse(logger, resp)...)
	}

	for _, req := range commands.alterPartitions {
		reqCp := req
		reqCp.Timeout = adminTimeout
		requestLogger.AlterPartitionReassignments(&reqCp)
		if !config.DryRun {
			resp, err := client.AlterPartitionReassignments(context.Background(), &reqCp)
			if err != nil {
				return err
			}
			itemErrs = append(itemErrs, EvalAlterPartitionReassignmentsResponse(logger, resp)...)
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

	return errors.Join(itemErrs...)
}

// EvalDeleteTopicsResponse logs and collects the per-topic errors kafka-go reports for DeleteTopics;
// a nil map entry means that topic's delete succeeded.
func EvalDeleteTopicsResponse(logger *slog.Logger, resp *kafka.DeleteTopicsResponse) (errs []error) {
	for topic, topicErr := range resp.Errors {
		if topicErr != nil {
			logger.Error("failed to delete topic", "topic", topic, "error", topicErr)
			errs = append(errs, fmt.Errorf("delete topic %v: %w", topic, topicErr))
		}
	}
	return errs
}

// EvalCreateTopicsResponse logs and collects the per-topic errors kafka-go reports for CreateTopics.
func EvalCreateTopicsResponse(logger *slog.Logger, resp *kafka.CreateTopicsResponse) (errs []error) {
	for topic, topicErr := range resp.Errors {
		if topicErr != nil {
			logger.Error("failed to create topic", "topic", topic, "error", topicErr)
			errs = append(errs, fmt.Errorf("create topic %v: %w", topic, topicErr))
		}
	}
	return errs
}

// EvalCreatePartitionsResponse logs and collects the per-topic errors kafka-go reports for
// CreatePartitions.
func EvalCreatePartitionsResponse(logger *slog.Logger, resp *kafka.CreatePartitionsResponse) (errs []error) {
	for topic, topicErr := range resp.Errors {
		if topicErr != nil {
			logger.Error("failed to create partitions", "topic", topic, "error", topicErr)
			errs = append(errs, fmt.Errorf("create partitions for topic %v: %w", topic, topicErr))
		}
	}
	return errs
}

// EvalAlterConfigsResponse logs and collects the per-resource errors kafka-go reports for
// AlterConfigs.
func EvalAlterConfigsResponse(logger *slog.Logger, resp *kafka.AlterConfigsResponse) (errs []error) {
	for resource, resourceErr := range resp.Errors {
		if resourceErr != nil {
			logger.Error("failed to alter config", "resource_type", resource.Type, "resource_name", resource.Name, "error", resourceErr)
			errs = append(errs, fmt.Errorf("alter config for %v: %w", resource.Name, resourceErr))
		}
	}
	return errs
}

// EvalAlterPartitionReassignmentsResponse logs and collects both the top-level and the per-partition
// errors kafka-go reports for AlterPartitionReassignments.
func EvalAlterPartitionReassignmentsResponse(logger *slog.Logger, resp *kafka.AlterPartitionReassignmentsResponse) (errs []error) {
	if resp.Error != nil {
		logger.Error("failed to reassign partitions", "error", resp.Error)
		errs = append(errs, fmt.Errorf("reassign partitions: %w", resp.Error))
	}
	for _, result := range resp.PartitionResults {
		if result.Error != nil {
			logger.Error("failed to reassign partition", "topic", result.Topic, "partition", result.PartitionID, "error", result.Error)
			errs = append(errs, fmt.Errorf("reassign topic %v partition %v: %w", result.Topic, result.PartitionID, result.Error))
		}
	}
	return errs
}

func Chunk(s []string, n int) [][]string {
	var chunk [][]string
	for i := 0; i < len(s); i += n {
		end := i + n
		if end > len(s) {
			end = len(s)
		}
		chunk = append(chunk, s[i:end])
	}
	return chunk
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

func CollectCommandsForTopic(config configuration.Config, broker []kafka.Broker, partitions []kafka.Partition, currentList configuration.TopicConfigs, topic configuration.TopicConfig) (commands Commands, err error) {
	if topic.Replicas > len(broker) {
		return commands, fmt.Errorf("topic %v tries to use replication-factor %v whole only %v brokers are known", topic.Name, topic.Replicas, len(broker))
	}

	var current configuration.TopicConfig
	for _, currentTopic := range currentList.Topics {
		if currentTopic.Name == topic.Name {
			current = currentTopic
			break
		}
	}

	topicPartitions := []kafka.Partition{}
	for _, partition := range partitions {
		if partition.Topic == topic.Name {
			topicPartitions = append(topicPartitions, partition)
		}
	}

	isUpdate := current.Partitions > 0
	if isUpdate {
		restartTopic := false

		if !reflect.DeepEqual(current.Config, topic.Config) {
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
		}

		//handle added partitions
		if current.Partitions < topic.Partitions {
			commands.createPartitions = append(commands.createPartitions, kafka.TopicPartitionsConfig{
				Name:                      topic.Name,
				Count:                     int32(topic.Partitions),
				TopicPartitionAssignments: nil,
			})
			restartTopic = true
		}

		// handle removed partitions
		if current.Partitions > topic.Partitions && config.AllowTopicDelete {
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
		if current.Replicas != topic.Replicas {
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
