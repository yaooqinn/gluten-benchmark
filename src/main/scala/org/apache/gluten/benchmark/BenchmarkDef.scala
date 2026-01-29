/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gluten.benchmark

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Definition of a single benchmark case.
 *
 * @param name        Human-readable name for the benchmark
 * @param cardinality Number of rows (for rate calculation)
 * @param workload    Function that creates the DataFrame to benchmark
 * @param setup       Optional setup function (create temp views, etc.)
 */
case class BenchmarkDef(
    name: String,
    cardinality: Long,
    workload: SparkSession => DataFrame,
    setup: Option[SparkSession => Unit] = None
) {

  /** Add setup step */
  def withSetup(setupFn: SparkSession => Unit): BenchmarkDef =
    copy(setup = Some(setupFn))
}

object BenchmarkDef {

  /**
   * Create a benchmark from a SQL query string.
   */
  def sql(name: String, cardinality: Long, query: String): BenchmarkDef =
    BenchmarkDef(name, cardinality, spark => spark.sql(query))

  /**
   * Create a benchmark from a SQL query with setup.
   */
  def sql(
      name: String,
      cardinality: Long,
      setupSql: String,
      query: String
  ): BenchmarkDef =
    BenchmarkDef(
      name,
      cardinality,
      spark => spark.sql(query),
      Some(spark => spark.sql(setupSql))
    )
}
