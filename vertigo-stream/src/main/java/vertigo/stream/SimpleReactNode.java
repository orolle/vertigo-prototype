/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package vertigo.stream;

import com.aol.simple.react.async.Queue;
import com.aol.simple.react.async.factories.QueueFactories;
import com.aol.simple.react.stream.lazy.LazyReact;
import com.aol.simple.react.stream.traits.LazyFutureStream;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import java.io.Serializable;
import vertigo.api.function.SerializableFunction;
import vertigo.api.function.Serializer;
import vertigo.core.dht.ComputeNode;

/**
 *
 * @author muhaaa
 */
public class SimpleReactNode<K extends Comparable<K> & Serializable, T extends Serializable> extends ComputeNode<K, T> {
  protected final LazyReact react = new LazyReact().
    withAsync(false);

  public SimpleReactNode(Vertx vertx, String prefix, K myHash) {
    super(vertx, prefix, myHash);
  }

  
  public SimpleReactNode<K, T> fromEventbus(K key, String fromAddress, String toAddress,
    SerializableFunction<LazyFutureStream<T>, LazyFutureStream<?>> builder,
    Handler<Future<Void>> deployed) {
    
    this.execute(key, p -> {
      EventBus eb = p.getVertx().eventBus();
      Queue<T> inQueue = QueueFactories.<T>unboundedQueue().build();
      
      LazyFutureStream<T> processingIn = ((SimpleReactNode<?,?>) p).react.fromStream(inQueue.streamCompletableFutures());
      
      LazyFutureStream<?> processingOut = builder.
        apply(processingIn).
        peek(data -> eb.publish(toAddress, data));
      
      processingOut.
        run();
      
      eb.<T>consumer(fromAddress).handler(msg -> {
        inQueue.offer(msg.body());
      });
    }, throwable -> {
      if (deployed != null) {
        if (throwable != null) {
          deployed.handle(Future.succeededFuture());
        } else {
          deployed.handle(Future.failedFuture(throwable));
        }
      }
    });

    return this;
  }
}


