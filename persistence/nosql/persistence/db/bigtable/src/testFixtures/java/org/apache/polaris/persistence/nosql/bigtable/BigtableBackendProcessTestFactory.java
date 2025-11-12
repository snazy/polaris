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
package org.apache.polaris.persistence.nosql.bigtable;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class BigtableBackendProcessTestFactory extends AbstractBigtableBackendTestFactory {
  public static final String NAME = "Bigtable-Process";

  public static final List<String> BIGTABLE_START_COMMAND =
      Arrays.stream(
              System.getProperty(
                      "polaris.testing.bigtable.command",
                      Optional.ofNullable(System.getenv("BIGTABLE_EMULATOR_COMMAND"))
                          .orElse("gcloud beta emulators bigtable start"))
                  .split(" "))
          .toList();
  public static final String BIGTABLE_HOST =
      System.getProperty(
          "polaris.testing.bigtable.host",
          Optional.ofNullable(System.getenv("BIGTABLE_EMULATOR_HOST")).orElse("localhost"));
  public static final int BIGTABLE_PORT =
      Integer.parseInt(
          System.getProperty(
              "polaris.testing.bigtable.port",
              Optional.ofNullable(System.getenv("BIGTABLE_EMULATOR_PORT")).orElse("0")));
  private Process process;
  private String emulatorHost;
  private int emulatorPort;

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public String getEmulatorHost() {
    return emulatorHost;
  }

  @Override
  public int getEmulatorPort() {
    return emulatorPort;
  }

  @Override
  public void start() {
    if (process != null && process.isAlive()) {
      throw new IllegalStateException("Already started");
    }

    var command =
        Stream.concat(
                BIGTABLE_START_COMMAND.stream(),
                Stream.of("--host-port=" + BIGTABLE_HOST + ":" + BIGTABLE_PORT))
            .collect(Collectors.toList());
    try {
      emulatorHost = BIGTABLE_HOST;
      emulatorPort = BIGTABLE_PORT;
      process =
          new ProcessBuilder(command)
              .redirectErrorStream(true)
              .redirectOutput(ProcessBuilder.Redirect.PIPE)
              .start();
      System.out.println("Cloud Bigtable emulator started with PID " + process.pid());
      if (emulatorPort == 0) {
        Pattern hostPortPattern =
            Pattern.compile(".*Cloud Bigtable emulator running on ([0-9:.]+):([0-9]+)");

        var p = process;
        var gotHostPort = new AtomicBoolean();
        var watch =
            new Thread(
                () -> {
                  try {
                    Thread.sleep(15_000);
                  } catch (InterruptedException e) {
                    destroyProcess(p);
                    throw new RuntimeException(e);
                  }
                  if (!gotHostPort.get()) {
                    System.err.println(
                        "Did not see 'Cloud Bigtable emulator running on ...' message after 15 seconds, stopping BT emulator process...");
                    destroyProcess(p);
                  }
                });
        watch.setDaemon(true);
        watch.start();

        var lines = new ArrayList<String>();
        try (var isr = new InputStreamReader(p.getInputStream(), UTF_8);
            var br = new BufferedReader(isr)) {
          while (true) {
            var line = br.readLine();
            if (line == null) {
              destroyProcess(p).forEach(ProcessHandle::destroyForcibly);
              throw new RuntimeException(
                  "Failed to start BigTable emulator, output so far: \n"
                      + String.join("\n", lines));
            }
            lines.add(line);
            var m = hostPortPattern.matcher(line);
            if (m.matches()) {
              emulatorHost = m.group(1);
              emulatorPort = Integer.parseInt(m.group(2));
              break;
            }
          }
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    projectId = "test-project";
    instanceId = "test-instance";
  }

  private List<ProcessHandle> destroyProcess(Process p) {
    var allProcesses =
        Stream.concat(Stream.of(p.toHandle()), p.descendants()).collect(Collectors.toList());
    for (var proc : allProcesses) {
      proc.destroy();
    }
    return allProcesses;
  }

  @Override
  public void stop() {
    try {
      if (process != null) {
        var p = process;
        var all = destroyProcess(p);
        try {
          if (!p.waitFor(30, TimeUnit.SECONDS)) {
            all.forEach(ProcessHandle::destroyForcibly);
          }
        } catch (InterruptedException e) {
          all.forEach(ProcessHandle::destroyForcibly);
          throw new RuntimeException(e);
        }
      }
    } finally {
      process = null;
    }
  }
}
