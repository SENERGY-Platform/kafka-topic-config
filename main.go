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

package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/SENERGY-Platform/kafka-topic-config/pkg"
	"github.com/SENERGY-Platform/kafka-topic-config/pkg/configuration"
	"github.com/SENERGY-Platform/kafka-topic-config/pkg/tests"
	"github.com/SENERGY-Platform/kafka-topic-config/pkg/tests/docker"
	"log"
	"strings"
	"sync"
	"time"
)

func main() {
	configLocation := flag.String("config", "config.json", "configuration file")
	test := flag.String("test", "", "comma seperated list of topic config yaml files. if set a local kafka cluster with 3 nodes will be started and all given files will be executed and tested")
	flag.Parse()

	conf, err := configuration.Load(*configLocation)
	if err != nil {
		log.Fatal("ERROR: unable to load config", err)
	}

	if test != nil && *test != "" {
		RunTest(conf, *test)
	} else {
		err = pkg.Run(conf)
		if err != nil {
			log.Fatal(err)
		}
	}
}

// RunTest
// starts a local kafka cluster with 3 nodes and tries to execute the given files and checks the results
func RunTest(conf configuration.Config, files string) {
	wg := &sync.WaitGroup{}
	defer wg.Wait()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fmt.Println("create kafka cluster")
	kafkaUrls, err := docker.KafkaCompose(ctx, wg)
	if err != nil {
		log.Fatal(err)
		return
	}
	conf.KafkaUrl = kafkaUrls[0]
	time.Sleep(1 * time.Second)

	for _, file := range strings.Split(files, ",") {
		conf.EnableKubernetesPodRestart = false
		conf.TopicConfigLocation = strings.TrimSpace(file)

		fmt.Println("execute", file)

		err = pkg.Run(conf)
		if err != nil {
			log.Fatal(err)
		}

		time.Sleep(5 * time.Second)

		fmt.Println("check ", file)
		err = tests.CheckKafkaState(nil, kafkaUrls, conf.TopicConfigLocation)
		if err != nil {
			log.Fatal(err)
		}
	}
}
