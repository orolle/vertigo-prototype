package vertigo.core.dht;

import io.vertx.core.eventbus.Message;

import java.io.Serializable;

import org.javatuples.Pair;

import vertigo.api.function.AsyncFunction;
import vertigo.api.function.ExecutionContext;
import vertigo.api.function.Serializer;


public interface Dht {

  public static <K extends Comparable<K>> String toAddress(String prefix, K hash) {
    final StringBuffer buf = new StringBuffer();
    buf.append(prefix).append(".").append(hash.toString());
    return buf.toString();
  }

  public static <K extends Comparable<K>> boolean isResponsible(DataNode<? extends K, ?> node, K hash) {
    return isResponsible(node.myKey, node.nextKey, hash);
  }

  public static <K extends Comparable<K>> boolean isResponsible(K my, K next, K hash) {
    // System.out.println(my+" "+next+" "+hash);
    
    // continued hash range
    if (my.compareTo(next) < 0) {
      return my.compareTo(hash) <= 0 && next.compareTo(hash) > 0;
    }
    // discontinued hash range
    else {
      return my.compareTo(hash) <= 0 || next.compareTo(hash) > 0;
    }
  }

  public static <K extends Comparable<K> & Serializable, T extends Serializable, R extends Serializable> byte[] managementMessage(
    AsyncFunction<Pair<ExecutionContext<DataNode<K, T>, Message<byte[]>, R>, Message<byte[]>>, R> f) {
    return Serializer.serialize(f);
  }
}
