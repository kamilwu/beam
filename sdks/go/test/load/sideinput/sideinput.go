// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"flag"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/io/synthetic"
	"github.com/apache/beam/sdks/go/pkg/beam/log"
	"github.com/apache/beam/sdks/go/pkg/beam/x/beamx"
	"github.com/apache/beam/sdks/go/test/load"
)

var (
	accessPercentage = flag.Int(
		"access_percentage",
		100,
		"Specifies the percentage of elements in the side input to be accessed.")
	syntheticSourceConfig = flag.String(
		"input_options",
		"{"+
			"\"num_records\": 300, "+
			"\"key_size\": 5, "+
			"\"value_size\": 15}",
		"A JSON object that describes the configuration for synthetic source")
)

func parseSyntheticConfig() synthetic.SourceConfig {
	if *syntheticSourceConfig == "" {
		panic("--input_options not provided")
	} else {
		encoded := []byte(*syntheticSourceConfig)
		return synthetic.DefaultSourceConfig().BuildFromJSON(encoded)
	}
}

func main() {
	flag.Parse()
	beam.Init()
	ctx := context.Background()
	p, s := beam.NewPipelineWithRoot()

	syntheticConfig := parseSyntheticConfig()
	elementsToAccess := syntheticConfig.NumElements * *accessPercentage / 100

	src := synthetic.SourceSingle(s, syntheticConfig)
	src = beam.ParDo(s, &load.RuntimeMonitor{}, src)

	src = beam.ParDo(s, func(_ []byte, values func(*[]byte, *[]byte) bool, emit func([]byte, []byte)) {
		var key []byte
		var value []byte
		i := 0
		for values(&key, &value) {
			if i == elementsToAccess {
				break
			}
			emit(key, value)
			i += 1
		}
	}, beam.Impulse(s), beam.SideInput{Input: src})

	beam.ParDo(s, &load.RuntimeMonitor{}, src)

	if err := beamx.Run(ctx, p); err != nil {
		log.Exitf(ctx, "Failed to execute job: %v", err)
	}
}
