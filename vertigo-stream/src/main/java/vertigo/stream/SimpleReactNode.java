/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package vertigo.stream;

import com.aol.simple.react.async.Queue;
import com.aol.simple.react.async.factories.QueueFactories;
import com.aol.simple.react.async.subscription.Continueable;
import com.aol.simple.react.config.MaxActive;
import com.aol.simple.react.stream.lazy.LazyReact;
import com.aol.simple.react.stream.lazy.ParallelReductionConfig;
import com.aol.simple.react.stream.traits.LazyFutureStream;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import java.io.Serializable;
import org.javatuples.Pair;
import vertigo.api.function.AsyncFunction;
import vertigo.api.function.SerializableConsumer;
import vertigo.api.function.SerializableFunction;
import vertigo.core.dht.ComputeNode;
import vertigo.core.dht.DataNode;
import vertigo.core.dht.ComputeMetric;

/**
 *
 * @author muhaaa
 */
public class SimpleReactNode<K extends Comparable<K> & Serializable> extends ComputeNode<K> {
  
  @Override
  public void join(Vertx vertx, String prefix, K key, Handler<DataNode<K, ComputeMetric>> started) {
    super.join(vertx, prefix, key, started);
  }
  
  public <T> SimpleReactNode<K> fromEventbus(K key, String fromAddress, String toAddress,
    SerializableFunction<LazyFutureStream<T>, LazyFutureStream<?>> builder,
    Handler<Future<Void>> deployed) {

    this.execute(key, p -> {
      EventBus eb = p.getVertx().eventBus();
      Queue<T> inQueue = QueueFactories.<T>unboundedQueue().build();
      LazyReact react = new LazyReact().
        withAsync(false);

      LazyFutureStream<T> processing = react.fromStream(inQueue.streamCompletableFutures());
      builder.
        apply(processing).
        peek(data -> eb.publish(toAddress, data)).
        run();

      eb.<T>consumer(fromAddress).handler(msg -> {
        inQueue.offer(msg.body());
      });
    }, throwable -> {
      if(deployed != null) {
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
