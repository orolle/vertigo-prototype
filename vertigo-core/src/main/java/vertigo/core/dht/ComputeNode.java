/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package vertigo.core.dht;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import java.io.Serializable;
import java.util.function.Consumer;
import vertigo.api.function.SerializableConsumer;

/**
 *
 * @author muhaaa
 */
public class ComputeNode<K extends Serializable & Comparable<K>, T extends Serializable> extends DataNode<K, T> {
  
  public ComputeNode(Vertx vertx, String prefix, K myHash) {
    super(vertx, prefix, myHash);
  }

  public void execute(K key, SerializableConsumer<ComputeNode<K, ?>> value, SerializableConsumer<Throwable> callback) {
    if (callback == null) {
      callback = b -> {
      };
    }

    this.traverse(key, key, (Throwable) null, (pair, v2) -> {
      pair.getValue1().vertx.runOnContext(h -> {
        try {
          value.accept((ComputeNode<K, T>) pair.getValue1());
          v2.accept(null);
        } catch (Exception e) {
          e.printStackTrace();
          v2.accept(e);
        }
      });
    }, callback);
  }
}
