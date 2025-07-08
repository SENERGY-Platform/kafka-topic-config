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
	"github.com/goccy/go-yaml"
	"os"
)

type TopicConfigs struct {
	Version int           `yaml:"version"`
	Topics  []TopicConfig `yaml:"topics"`
}

type TopicConfig struct {
	Name       string               `yaml:"name"`
	Partitions int                  `yaml:"partitions"`
	Replicas   int                  `yaml:"replicas"`
	Config     map[string]string    `yaml:"config,omitempty"`
	Restart    []TopicConfigRestart `yaml:"restart,omitempty"`
}

type TopicConfigRestart struct {
	Namespace string `yaml:"namespace"`
	App       string `yaml:"app"`
}

func LoadTopicConfigsFromYaml(location string) (conf TopicConfigs, err error) {
	buff, err := os.ReadFile(location)
	if err != nil {
		return conf, err
	}
	err = yaml.Unmarshal(buff, &conf)
	if err != nil {
		return conf, err
	}
	conf.SetDefaults()
	return conf, nil
}

func (this *TopicConfigs) SetDefaults() {
	for i, v := range this.Topics {
		v.SetDefaults()
		this.Topics[i] = v
	}
}

func (this *TopicConfig) SetDefaults() {
	if this.Partitions == 0 {
		this.Partitions = 1
	}
	if this.Replicas == 0 {
		this.Replicas = 1
	}
	if this.Restart == nil {
		this.Restart = []TopicConfigRestart{}
	}
	if this.Config == nil {
		this.Config = map[string]string{}
	}
}
