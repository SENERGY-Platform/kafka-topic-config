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
	"iter"
	"log"
	"slices"
	"strings"

	"github.com/SENERGY-Platform/device-repository/lib/client"
	"github.com/SENERGY-Platform/kafka-topic-config/pkg/configuration"
	"github.com/SENERGY-Platform/models/go/models"
	"github.com/segmentio/kafka-go"
)

func CheckDeviceTypeTopics(config configuration.Config, partitions []kafka.Partition) (commands Commands, err error) {
	if config.DeviceRepositoryUrl == "" || config.DeviceRepositoryUrl == "-" {
		log.Println("WARNING: DeviceRepositoryUrl not set --> skipping device-type topic check")
		return commands, nil
	}
	if config.ServiceTopicPrefix == "" || config.ServiceTopicPrefix == "-" {
		log.Println("WARNING: ServiceTopicPrefix not set --> skipping device-type topic check")
		return commands, nil
	}
	getBatch := func(limit int64, offset int64) ([]models.DeviceType, error) {
		list, _, err, _ := client.NewClient(config.DeviceRepositoryUrl, nil).ListDeviceTypesV3(client.InternalAdminToken, client.DeviceTypeListOptions{
			Limit:  limit,
			Offset: offset,
		})
		return list, err
	}

	expectedServiceTopics := map[string]bool{}
	for dt, err := range Iter(100, getBatch) {
		if err != nil {
			return commands, err
		}
		for _, service := range dt.Services {
			expectedServiceTopics[ServiceIdToTopic(service.Id)] = true
		}
	}
	serviceTopics := []string{}
	for _, partition := range partitions {
		if strings.HasPrefix(partition.Topic, config.ServiceTopicPrefix) && !slices.Contains(serviceTopics, partition.Topic) {
			serviceTopics = append(serviceTopics, partition.Topic)
		}
	}

	for _, topic := range serviceTopics {
		if !expectedServiceTopics[topic] {
			commands.deleteTopics = append(commands.deleteTopics, topic)
		}
	}
	return commands, nil
}

func ServiceIdToTopic(id string) string {
	id = strings.ReplaceAll(id, "#", "_")
	id = strings.ReplaceAll(id, ":", "_")
	return id
}

func Iter[T any](batchsize int64, getBatch func(limit int64, offset int64) ([]T, error)) iter.Seq2[T, error] {
	var offset int64 = 0
	return func(yield func(T, error) bool) {
		finished := false
		for !finished {
			batch, err := getBatch(batchsize, offset)
			if err != nil {
				var element T
				yield(element, err)
				return
			}
			for _, instance := range batch {
				if !yield(instance, nil) {
					return
				}
			}
			offset += batchsize
			if len(batch) < int(batchsize) {
				finished = true
			}
		}
	}
}
