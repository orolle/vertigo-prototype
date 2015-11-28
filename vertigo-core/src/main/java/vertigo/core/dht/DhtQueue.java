/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package vertigo.core.dht;

import io.vertx.core.Vertx;
import java.io.Serializable;

/**
 *
 * @author muhaaa
 * @param <K>
 * @param <T>
 */
public class DhtQueue<K extends Comparable<K> & Serializable, T extends Serializable> extends DataNode<K, T> {
  
  
  public DhtQueue(Vertx vertx, String prefix, K myHash) {
    super(vertx, prefix, myHash);
  }
  
}
