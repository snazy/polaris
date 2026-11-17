/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.polaris.persistence.nosql.impl.indexes;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class TestProductionIndexJourney {
  @ParameterizedTest
  @EnumSource(RepresentativeKeyDataset.Family.class)
  void productionFactoryJourneysValidateModelsAndVersionRoles(
      RepresentativeKeyDataset.Family family) {
    for (String scenario :
        new String[] {"EMBEDDED", "STRIPED_SPILL", "DEFAULT_UPDATE", "DEFAULT_SPILL"}) {
      int entries =
          scenario.equals("EMBEDDED") ? 64 : scenario.startsWith("DEFAULT_") ? 4096 : 1024;
      var workload =
          RepresentativeIndexWorkload.create(
              family,
              1369956643L,
              RepresentativeIndexWorkload.Variant.BASELINE,
              entries,
              0,
              RepresentativeIndexWorkload.History.FRESH,
              RepresentativeIndexWorkload.Partitioning.NATIVE);
      for (String variant : new String[] {"BASELINE", "PRODUCTION_V2", "PROMOTE_V2"})
        verify(workload, variant, scenario, entries);
    }
  }

  private static <V> void verify(
      RepresentativeIndexWorkload<V> workload, String variant, String scenario, int entries) {
    var journey = new ProductionIndexJourney<>(workload, variant, scenario, 16);
    var params =
        Map.of(
            "family",
            workload.dataset.config().family().name(),
            "seed",
            "1369956643",
            "variant",
            variant,
            "scenario",
            scenario);
    for (boolean write : new boolean[] {false, true}) {
      var data = journey.metadata(params, write, write ? "writeRequest" : "readRequest");
      assertThat(data.get("inputVersion")).isEqualTo(variant.equals("PRODUCTION_V2") ? 2 : 1);
      assertThat(data.get("outputVersion"))
          .isEqualTo(
              variant.equals("PRODUCTION_V2") || (write && variant.equals("PROMOTE_V2")) ? 2 : 1);
      assertThat(data.get("outputEntries")).isEqualTo(write ? entries - 1 : entries);
      if (scenario.startsWith("DEFAULT_")) {
        assertThat(data.get("configuredEmbeddedLimit")).isEqualTo(32768L);
        assertThat(data.get("configuredStripeLimit")).isEqualTo(131072L);
        assertThat((int) data.get("fetchedStripes")).isBetween(1, (int) data.get("inputStripes"));
        if (write && scenario.equals("DEFAULT_UPDATE"))
          assertThat((int) data.get("writtenStripes")).isZero();
        if (scenario.equals("DEFAULT_SPILL"))
          assertThat((int) data.get("initialEmbeddedBytes")).isBetween(32000, 32768);
      } else
        assertThat((int) data.get("fetchedStripes")).isEqualTo(scenario.equals("EMBEDDED") ? 0 : 2);
      if (write && (scenario.equals("STRIPED_SPILL") || scenario.equals("DEFAULT_SPILL")))
        assertThat((int) data.get("writtenStripes")).isPositive();
      // A second operation starts from the same immutable input and stable synthetic IDs.
      assertThat(journey.metadata(params, write, write ? "writeRequest" : "readRequest"))
          .isEqualTo(data);
    }
  }
}
