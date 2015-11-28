/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package vertigo.core.dht;

import io.vertx.core.Vertx;
import java.io.Serializable;
import vertigo.api.function.SerializableConsumer;

/**
 *
 * @author muhaaa
 */
public class ComputeClient<K extends Serializable & Comparable<K>, T extends Serializable> extends DataClient<K, T>{

  public ComputeClient(Vertx vertx, String prefix) {
    super(vertx, prefix);
  }
  
  public void execute(K key, SerializableConsumer<DataNode<K, ?>> value, SerializableConsumer<Throwable> callback) {
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
