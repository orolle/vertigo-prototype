package vertigo.core.dht;

import io.vertx.core.AsyncResult;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;

import java.io.Serializable;

import org.javatuples.Pair;

import vertigo.api.function.AsyncFunction;
import vertigo.api.function.SerializableConsumer;
import vertigo.api.function.ExecutionContext;

public class DataNode<K extends Comparable<K> & Serializable, T extends Serializable> {

  protected final Vertx vertx;
  protected final String prefix;
  protected final K myKey;
  protected K nextKey;

  protected AsyncMap<K, T> values = new AsyncMap<>(this);

  public DataNode(Vertx vertx, String prefix, K myHash) {
    this.vertx = vertx;
    this.prefix = prefix;
    this.myKey = myHash;
    this.nextKey = myHash;
  }

  public DataNode<K, T> join(SerializableConsumer<DataNode<K, T>> joined) {
    new DataNodeBootstrap<>(this).
      onSuccess(nextHash -> this.nextKey = nextHash).
      onSuccess(nextHash -> onBootstraped()).
      onSuccess(nextHash -> joined.accept(this)).
      bootstrap();

    return this;
  }

  protected void onBootstraped() {
    vertx.eventBus().consumer(Dht.toAddress(prefix, 0), (Message<byte[]> msg) -> processManagementMessage(msg));
    vertx.eventBus().consumer(Dht.toAddress(prefix, myKey), (Message<byte[]> msg) -> processManagementMessage(msg));
  }

  public K getIdentity() {
    return myKey;
  }

  public K getNext() {
    return this.nextKey;
  }

  public Vertx getVertx() {
    return vertx;
  }

  public AsyncMap<K, T> getValues() {
    return this.values;
  }

  protected void processManagementMessage(Message<byte[]> msg) {
    ExecutionContext<DataNode<K, T>, Message<byte[]>, Void> l = new ExecutionContext<>(msg.body());
    l.context(this);
    l.onNext(msg);
  }

  public <R extends Serializable> void traverse(K start, K end, R identity,
    AsyncFunction<Pair<ExecutionContext<Void, DataNode<K, T>, R>, DataNode<K, T>>, R> f,
    SerializableConsumer<R> handler) {
    final K key = getIdentity();

    byte[] ser = Dht.<K, T, R>managementMessage((pair, cb) -> {
      ExecutionContext<DataNode<K, T>, Message<byte[]>, R> c = pair.getValue0();
      Message<byte[]> msg = pair.getValue1();

      if ((!start.equals(end) && Dht.isResponsible(start, end, c.context().myKey))
        || Dht.isResponsible(c.context(), start)
        || Dht.isResponsible(c.context(), end)) {
        Pair<ExecutionContext<Void, DataNode<K, T>, R>, DataNode<K, T>> p = new Pair<>(
          new ExecutionContext<Void, DataNode<K, T>, R>(f),
          pair.getValue0().context());
        f.apply(p, (R result) -> {
          msg.reply(result);
        });
      }

      if (!key.equals(c.context().myKey)) {
        String addr = Dht.toAddress(c.context().prefix, c.context().nextKey);
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
    });

    vertx.eventBus().send(Dht.toAddress(prefix, nextKey), ser, (AsyncResult<Message<R>> ar) -> {
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

  @Override
  public String toString() {
    return this.getIdentity().toString() + ": ["
      + this.getIdentity().toString() + "-"
      + this.getNext().toString() + "]";
  }
}
