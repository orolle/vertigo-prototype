/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package vertigo.core.dht;

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
public class IData {

  public static <K extends Comparable<K> & Serializable, T extends Serializable, R extends Serializable>
    byte[]
    tranverseMessage(K key, K start, K end, R identity, AsyncFunction<Pair<ExecutionContext<Void, DataNode<K, T>, R>, DataNode<K, T>>, R> f,
      SerializableConsumer<R> handler) {
    return IDht.<K, T, R>functionToMessage(IData.<K, T, R>tranverse(key, start, end, identity, f, handler));
  }

  /**
   *
   * @param <K> The key type of the DHT
   * @param <T> The value type of the DHT
   * @param <R> The result type of the function parameter "function"
   * @param key The key of the inital node to terminate traverse if inital node is reached
   * @param start Beginning of key space for traverse. Traverse key space between [start, end]
   * @param end Termination of key space for traverse. Traverse key space between [start, end]
   * @param identity Default return value if traverse is terminated because inital node is reached
   * @param function Function to apply within key space
   * @param handler Result handler after traverse is completed
   * @return The traverse function
   */
  public static <K extends Comparable<K> & Serializable, T extends Serializable, R extends Serializable>
    AsyncFunction<Pair<ExecutionContext<DataNode<K, T>, Message<byte[]>, R>, Message<byte[]>>, R>
    tranverse(K key, K start, K end, R identity, AsyncFunction<Pair<ExecutionContext<Void, DataNode<K, T>, R>, DataNode<K, T>>, R> function,
      SerializableConsumer<R> handler) {
    return (pair, cb) -> {
      ExecutionContext<DataNode<K, T>, Message<byte[]>, R> c = pair.getValue0();
      Message<byte[]> msg = pair.getValue1();

      if ((!start.equals(end) && IDht.isResponsible(start, end, c.context().myKey))
        || IDht.isResponsible(c.context(), start)
        || IDht.isResponsible(c.context(), end)) {
        Pair<ExecutionContext<Void, DataNode<K, T>, R>, DataNode<K, T>> p = new Pair<>(
          new ExecutionContext<Void, DataNode<K, T>, R>(function),
          pair.getValue0().context());
        function.apply(p, (R result) -> {
          msg.reply(result);
        });
      }

      if (!c.context().myKey.equals(key)) {
        String addr = IDht.toAddress(c.context().prefix, c.context().nextKey);
        c.context().vertx.eventBus().send(addr, c.serialize(), ar -> {
          if (ar.succeeded()) {
            msg.reply(ar.result().body());
          } else {
            msg.reply(ar.cause());
          }
        });
      } else {
        msg.reply(identity);
      }
    };
  }
}
