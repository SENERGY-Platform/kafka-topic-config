/*
 * Copyright 2026 InfAI (CC SES)
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
	"strings"
	"testing"
	"time"

	"github.com/SENERGY-Platform/kafka-topic-config/pkg/configuration"
	"github.com/segmentio/kafka-go"
)

// fakeKafkaClient implements KafkaClient with canned responses, so ExecuteCommands can be tested
// without a running kafka cluster.
type fakeKafkaClient struct {
	deleteTopicsResp     *kafka.DeleteTopicsResponse
	createTopicsResp     *kafka.CreateTopicsResponse
	alterConfigsResp     *kafka.AlterConfigsResponse
	createPartitionsResp *kafka.CreatePartitionsResponse
	alterPartitionsResp  *kafka.AlterPartitionReassignmentsResponse

	lastAlterPartitionsReq *kafka.AlterPartitionReassignmentsRequest
}

func (f *fakeKafkaClient) DeleteTopics(_ context.Context, _ *kafka.DeleteTopicsRequest) (*kafka.DeleteTopicsResponse, error) {
	if f.deleteTopicsResp != nil {
		return f.deleteTopicsResp, nil
	}
	return &kafka.DeleteTopicsResponse{}, nil
}

func (f *fakeKafkaClient) CreateTopics(_ context.Context, _ *kafka.CreateTopicsRequest) (*kafka.CreateTopicsResponse, error) {
	if f.createTopicsResp != nil {
		return f.createTopicsResp, nil
	}
	return &kafka.CreateTopicsResponse{}, nil
}

func (f *fakeKafkaClient) AlterConfigs(_ context.Context, _ *kafka.AlterConfigsRequest) (*kafka.AlterConfigsResponse, error) {
	if f.alterConfigsResp != nil {
		return f.alterConfigsResp, nil
	}
	return &kafka.AlterConfigsResponse{}, nil
}

func (f *fakeKafkaClient) CreatePartitions(_ context.Context, _ *kafka.CreatePartitionsRequest) (*kafka.CreatePartitionsResponse, error) {
	if f.createPartitionsResp != nil {
		return f.createPartitionsResp, nil
	}
	return &kafka.CreatePartitionsResponse{}, nil
}

func (f *fakeKafkaClient) AlterPartitionReassignments(_ context.Context, k *kafka.AlterPartitionReassignmentsRequest) (*kafka.AlterPartitionReassignmentsResponse, error) {
	f.lastAlterPartitionsReq = k
	if f.alterPartitionsResp != nil {
		return f.alterPartitionsResp, nil
	}
	return &kafka.AlterPartitionReassignmentsResponse{}, nil
}

// TestExecuteCommandsSetsAlterPartitionReassignmentsTimeout guards the fix for the production
// incident: kafka-go turns an unset Timeout into TimeoutMs=0, which a KRaft controller refuses
// immediately, so the request must always carry the configured admin timeout.
func TestExecuteCommandsSetsAlterPartitionReassignmentsTimeout(t *testing.T) {
	fake := &fakeKafkaClient{}
	commands := Commands{
		alterPartitions: []kafka.AlterPartitionReassignmentsRequest{
			{
				Topic: "reassign-me",
				Assignments: []kafka.AlterPartitionReassignmentsRequestAssignment{
					{Topic: "reassign-me", PartitionID: 0, BrokerIDs: []int{1, 2}},
				},
			},
		},
	}

	wantTimeout := 42 * time.Second
	err := ExecuteCommands(configuration.Config{}, nil, fake, &KafkaClientRequestLogger{}, wantTimeout, commands)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if fake.lastAlterPartitionsReq == nil {
		t.Fatal("AlterPartitionReassignments was not called")
	}
	if fake.lastAlterPartitionsReq.Timeout != wantTimeout {
		t.Errorf("Timeout = %v, want %v", fake.lastAlterPartitionsReq.Timeout, wantTimeout)
	}
}

// TestExecuteCommandsFailsOnPartitionReassignmentError guards against the same incident from the
// other side: a per-partition error inside an otherwise transport-successful response must fail
// the run and name the affected topic and partition.
func TestExecuteCommandsFailsOnPartitionReassignmentError(t *testing.T) {
	fake := &fakeKafkaClient{
		alterPartitionsResp: &kafka.AlterPartitionReassignmentsResponse{
			PartitionResults: []kafka.AlterPartitionReassignmentsResponsePartitionResult{
				{Topic: "reassign-me", PartitionID: 2, Error: errors.New("event unable to start processing because of TimeoutException")},
			},
		},
	}
	commands := Commands{
		alterPartitions: []kafka.AlterPartitionReassignmentsRequest{
			{
				Topic: "reassign-me",
				Assignments: []kafka.AlterPartitionReassignmentsRequestAssignment{
					{Topic: "reassign-me", PartitionID: 2, BrokerIDs: []int{1, 2}},
				},
			},
		},
	}

	err := ExecuteCommands(configuration.Config{}, nil, fake, &KafkaClientRequestLogger{}, time.Minute, commands)
	if err == nil {
		t.Fatal("expected an error, got nil")
	}
	if !strings.Contains(err.Error(), "reassign-me") {
		t.Errorf("error %q does not name the topic", err.Error())
	}
	if !strings.Contains(err.Error(), "2") {
		t.Errorf("error %q does not name the partition", err.Error())
	}
}

// TestExecuteCommandsFailsOnCreateTopicsError is the same check for CreateTopics: a per-topic
// error must fail the run and name the topic, even though the transport call itself succeeded.
func TestExecuteCommandsFailsOnCreateTopicsError(t *testing.T) {
	fake := &fakeKafkaClient{
		createTopicsResp: &kafka.CreateTopicsResponse{
			Errors: map[string]error{
				"new-topic": errors.New("replication factor larger than available brokers"),
			},
		},
	}
	commands := Commands{
		createTopics: []kafka.TopicConfig{
			{Topic: "new-topic", NumPartitions: 3, ReplicationFactor: 3},
		},
	}

	err := ExecuteCommands(configuration.Config{}, nil, fake, &KafkaClientRequestLogger{}, time.Minute, commands)
	if err == nil {
		t.Fatal("expected an error, got nil")
	}
	if !strings.Contains(err.Error(), "new-topic") {
		t.Errorf("error %q does not name the topic", err.Error())
	}
}
