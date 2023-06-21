/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.blobstore;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

public class ShardIdTranslog {
  public static Optional<Integer> shardId(String filePath) {
    try {
      return Files.lines(Path.of(filePath)).map(Integer::valueOf).reduce((a, b) -> b);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
