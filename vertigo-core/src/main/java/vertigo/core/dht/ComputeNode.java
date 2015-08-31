/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package vertigo.core.dht;

import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Random;
import java.util.function.Consumer;
import org.javatuples.Pair;
import vertigo.api.function.AsyncFunction;
import vertigo.api.function.ExecutionContext;
import vertigo.api.function.SerializableConsumer;

/**
 *
 * @author muhaaa
 */
public class ComputeNode<K extends Serializable & Comparable<K>> {

  protected DataNode<K, ComputeMetric> node;
  //protected DhtNode<LazyReact> noder;
  protected Vertx vertx;

  public void join(Vertx vertx, String prefix, K key, Handler<DataNode<K, ComputeMetric>> started) {
    this.vertx = vertx;
    this.node = new DataNode<>(vertx, prefix, key);
    this.node.join(node -> started.handle(node));
  }

  public void execute(K key, SerializableConsumer<DataNode<K, ?>> value, SerializableConsumer<Throwable> callback) {
    if (callback == null) {
      callback = b -> {
      };
    }

    this.node.traverse(key, key, (Throwable) null, (pair, v2) -> {
      pair.getValue1().getValues().put(key, new ComputeMetric());
      pair.getValue1().vertx.runOnContext(h -> {
        try {
          value.accept(pair.getValue1());
          v2.accept(null);
        } catch (Exception e) {
          v2.accept(e);
        }
      });
    }, callback);
  }
}
