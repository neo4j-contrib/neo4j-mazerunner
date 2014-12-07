package org.mazerunner.core

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{PartitionStrategy, GraphLoader}

/**
 * Copyright (C) 2014 Kenny Bastani
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
object algorithms {

  def connectedComponents(sc: SparkContext, path: String) : String = {
    val graph = GraphLoader.edgeListFile(sc, path);

    val v = graph.connectedComponents().vertices

    val results = v.map { row =>
      row._1 + " " + row._2
    }

    results.collect().mkString("\n")
  }

  def pageRank(sc: SparkContext, path: String) : String = {
    val graph = GraphLoader.edgeListFile(sc, path);

    val v = graph.staticPageRank(2, 0.001).vertices

    val results = v.map { row =>
      row._1 + " " + row._2
    }

    results.collect().mkString("\n")
  }

  def stronglyConnectedComponents(sc: SparkContext, path: String) : String = {
    val graph = GraphLoader.edgeListFile(sc, path);

    val v = graph.stronglyConnectedComponents(10).vertices

    val results = v.map { row =>
      row._1 + " " + row._2
    }

    results.collect().mkString("\n")
  }

  def triangleCount(sc: SparkContext, path: String) : String = {
    val graph = GraphLoader.edgeListFile(sc, path, true).partitionBy(PartitionStrategy.RandomVertexCut)

    val v = graph.triangleCount().vertices

    val results = v.map { row =>
      row._1 + " " + row._2
    }

    results.collect().mkString("\n")
  }

}
