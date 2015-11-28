/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package vertigo.core.dht;

import io.vertx.core.AsyncResult;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import java.io.Serializable;
import org.javatuples.Pair;
import vertigo.api.function.AsyncFunction;
import vertigo.api.function.ExecutionContext;
import vertigo.api.function.SerializableConsumer;

/**
 *
 * @author muhaaa
 */
public class DataClient<K extends Serializable & Comparable<K>, T extends Serializable> {

  protected final Vertx vertx;
  protected final String prefix;

  public DataClient(Vertx vertx, String prefix) {
    this.vertx = vertx;
    this.prefix = prefix;
  }

  public <R extends Serializable> void traverse(K start, K end, R identity,
    AsyncFunction<Pair<ExecutionContext<Void, DataNode<K, T>, R>, DataNode<K, T>>, R> f,
    SerializableConsumer<R> handler) {
    byte[] ser = IData.<K, T, R>tranverseMessage(null, start, end, identity, f, handler);
    
    vertx.eventBus().send(IDht.toAddress(prefix, ""), ser, (AsyncResult<Message<R>> ar) -> {
      if (handler != null) {
        if (ar.succeeded()) {
          handler.accept(ar.result().body());
        } else {
          handler.accept(null);
        }
      }
    });
  }
  
  public void put(K key, T value, SerializableConsumer<Boolean> callback) {
    traverse(key, key, Boolean.TRUE, (pair, v2) -> {
      pair.getValue1().getValues().put(key, value);
      v2.accept(true);
    }, callback);
  }

  public void get(K key, SerializableConsumer<T> callback) {
    traverse(key, key, null, (pair, cb) -> {
      T data = pair.getValue1().getValues().get(key);
      cb.accept(data);
    }, callback);
  }
}
