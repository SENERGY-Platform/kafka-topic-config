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

package configuration

import (
	"github.com/SENERGY-Platform/go-service-base/config-hdl"
)

type Config struct {
	ServiceTopicPrefix                      string `json:"service_topic_prefix" env_var:"SERVICE_TOPIC_PREFIX"`
	DeviceRepositoryUrl                     string `json:"device_repository_url" env_var:"DEVICE_REPOSITORY_URL"`
	KafkaUrl                                string `json:"kafka_url" env_var:"KAFKA_URL"`
	TopicConfigLocation                     string `json:"topic_config_location" env_var:"TOPIC_CONFIG_LOCATION"`
	AllowTopicDelete                        bool   `json:"allow_topic_delete" env_var:"ALLOW_TOPIC_DELETE"`
	EnableKubernetesPodRestart              bool   `json:"enable_kubernetes_pod_restart" env_var:"ENABLE_KUBERNETES_POD_RESTART"`
	DryRun                                  bool   `json:"dry_run" env_var:"DRY_RUN"`
	LogCurrentState                         bool   `json:"log_current_state" env_var:"LOG_CURRENT_STATE"`
	LogCurrentStateToFile                   string `json:"log_current_state_to_file" env_var:"LOG_CURRENT_STATE_TO_FILE"`
	LogCurrentStateIfTopicMatchesRegex      string `json:"log_current_state_if_topic_matches_regex" env_var:"LOG_CURRENT_STATE_IF_TOPIC_MATCHES_REGEX"`
	LogCurrentStateIfTopicDoesNotMatchRegex string `json:"log_current_state_if_topic_does_not_match_regex" env_var:"LOG_CURRENT_STATE_IF_TOPIC_DOES_NOT_MATCH_REGEX"`
}

func Load(location string) (conf Config, err error) {
	err = config_hdl.Load(&conf, nil, nil, nil, location)
	return conf, err
}
