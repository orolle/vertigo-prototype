package vertigo.core.dht;

import io.vertx.core.AsyncResult;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.Message;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class DataNodeBootstrap<K extends Comparable<K> & Serializable, T extends Serializable> {
  protected final DataNode<K, T> node;
  protected final List<Consumer<K>> on = new ArrayList<>();

  public DataNodeBootstrap(DataNode<K, T> node) {
    this.node = node;
  }
  
  public DataNodeBootstrap<K, T> onSuccess(Consumer<K> c) {
    on.add(c);
    return this;
  }

  public void bootstrap() {
    final K hash = node.myKey;

    byte[] ser = Dht.managementMessage((pair, cb) -> {
      DataNode<K, ?> context = (DataNode<K, ?>) pair.getValue0().context();
      Message<byte[]> msg = pair.getValue1();

      if (Dht.isResponsible(context, hash)) {
        msg.reply(context.nextKey);
        context.nextKey = hash;
      } else {
        context.vertx.eventBus().send(Dht.toAddress(context.prefix, context.nextKey), pair.getValue0().serialize(),
          ar -> {
            if (ar.succeeded()) {
              msg.reply(ar.result().body());
            }
          });
      }

      cb.accept(null);
    });
    
    node.vertx.eventBus().send(Dht.toAddress(node.prefix, 0),
      ser,
      new DeliveryOptions().setSendTimeout(10000),
      (AsyncResult<Message<K>> ar) -> {
        on.forEach(c -> c.accept(ar.succeeded() ? ar.result().body() : hash));
      });
  }
}
