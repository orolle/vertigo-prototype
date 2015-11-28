package vertigo.core.dht;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;

import java.io.Serializable;
import java.util.function.Consumer;

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

  public void join(Consumer<Future<Void>> joined) {
    new DataNodeBootstrap<>(this).
      onSuccess(this::onBootstraped).
      onSuccess(nextHash -> joined.accept(Future.succeededFuture(null))).
      bootstrap();
  }

  protected void onBootstraped(K nextHash) {
    this.nextKey = nextHash;
    vertx.eventBus().consumer(IDht.toAddress(prefix, ""), (Message<byte[]> msg) -> processManagementMessage(msg));
    vertx.eventBus().consumer(IDht.toAddress(prefix, myKey), (Message<byte[]> msg) -> processManagementMessage(msg));
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
    byte[] ser = IData.<K, T, R> tranverseMessage(myKey, start, end, identity, f, handler);
    
    vertx.eventBus().send(IDht.toAddress(prefix, nextKey), ser, (AsyncResult<Message<R>> ar) -> {
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
