// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import static java.nio.charset.StandardCharsets.UTF_8;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class HdfsEnvTest {
  @ClassRule
  public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE =
      new RocksNativeLibraryResource();

  @Rule public TemporaryFolder dbFolder = new TemporaryFolder();

  /**
   * Connect to a running hdfs
   *
   * @throws RocksDBException
   */
  @Test
  public void construct() throws RocksDBException {
    try (final Env env = new HdfsEnv("hdfs://localhost:5000")) {
      // no-op
    }
  }

  /**
   * Connect to a running hdfs and do a simple put
   *
   * @throws RocksDBException
   */
  @Test
  public void construct_integration() throws RocksDBException {
    try (final Env env = new HdfsEnv("hdfs://localhost:5000");
         final Options options = new Options().setCreateIfMissing(true).setEnv(env);) {
      try (final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getPath())) {
        db.put("key1".getBytes(UTF_8), "value1".getBytes(UTF_8));
      }
    }
  }
}
