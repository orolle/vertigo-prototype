package vertigo.core.dht;

import java.io.Serializable;
import java.util.Map.Entry;

import org.javatuples.Pair;

import vertigo.api.function.AsyncFunction;

public interface AsyncMapCallback<K extends Comparable<K> & Serializable, V extends Serializable>
  extends AsyncFunction<Pair<DataNode<K, V>, Entry<K, V>>, Void> {
}
