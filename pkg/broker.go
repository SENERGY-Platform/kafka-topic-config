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
	"github.com/segmentio/kafka-go"
	"net"
	"strconv"
)

func GetBrokerInfo(kafkaUrl string) (brokers []kafka.Broker, partitions []kafka.Partition, err error) {
	conn, err := kafka.Dial("tcp", kafkaUrl)
	if err != nil {
		return nil, nil, err
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return nil, nil, err
	}
	var controllerConn *kafka.Conn
	controllerConn, err = kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		return nil, nil, err
	}
	defer controllerConn.Close()

	partitions, err = controllerConn.ReadPartitions()
	if err != nil {
		return nil, nil, err
	}
	brokers, err = controllerConn.Brokers()
	if err != nil {
		return nil, nil, err
	}

	return brokers, partitions, nil
}

func BrokerToAddrList(brokers []kafka.Broker) (result []string, err error) {
	for _, broker := range brokers {
		result = append(result, net.JoinHostPort(broker.Host, strconv.Itoa(broker.Port)))
	}
	return result, nil
}
