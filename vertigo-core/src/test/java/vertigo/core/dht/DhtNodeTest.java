/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package vertigo.core.dht;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.runner.RunWith;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import java.util.Random;
import java.util.UUID;
import java.util.function.Supplier;
import org.javatuples.Pair;
import vertigo.core.dht.DataNode;

/**
 *
 * @author muhaaa
 */
@RunWith(VertxUnitRunner.class)
public class DhtNodeTest {

  Vertx vertx;

  public DhtNodeTest() {
  }

  @BeforeClass
  public static void setUpClass() {
  }

  @AfterClass
  public static void tearDownClass() {
  }

  @Before
  public void before(TestContext context) {
    vertx = Vertx.vertx();
  }

  @After
  public void after(TestContext context) {
    vertx.close(context.asyncAssertSuccess());
  }

  /*
   *   Tests
   */
  @Test
  public void singleDhtNode(TestContext context) {
    Async test = context.async();
    DataNode<Integer, String> n = new DataNode<>(vertx, "prefix", Integer.MAX_VALUE);
    Integer key = Integer.MAX_VALUE / 2;
    String value = "Hello World!";

    n.join(node -> {
      node.put(key, value, cb -> {
      });
      node.get(key, val -> {
        context.assertEquals(value, val);
        test.complete();
      });
    });
  }

  @Test
  public void multiDhtNodes(TestContext context) {
    Async test = context.async();
    Random rand = new Random(123456);
    Integer key = Integer.MAX_VALUE / 2;
    String value = "Hello World!";

    new DataNode<>(vertx, "prefix", rand.nextInt(Integer.MAX_VALUE)).join(n1 -> {
      new DataNode<>(vertx, "prefix", rand.nextInt(Integer.MAX_VALUE)).join(n2 -> {
        new DataNode<>(vertx, "prefix", rand.nextInt(Integer.MAX_VALUE)).join(node -> {
          node.put(key, value, cb -> {
            node.get(key, val -> {
              context.assertEquals(value, val);
              test.complete();
            });
          });
        });
      });
    });
  }

  @Test
  public void multiDhtNodesStringKey(TestContext context) {
    Async test = context.async();
    Random rand = new Random(123456);
    byte[] bytes = new byte[8];
    rand.nextBytes(bytes);
    String key = UUID.nameUUIDFromBytes(bytes).toString();
    String value = "Hello World!";

    rand.nextBytes(bytes);
    new DataNode<>(vertx, "prefix", UUID.nameUUIDFromBytes(bytes).toString()).join(n1 -> {
      rand.nextBytes(bytes);
      new DataNode<>(vertx, "prefix", UUID.nameUUIDFromBytes(bytes).toString()).join(n2 -> {
        rand.nextBytes(bytes);
        new DataNode<>(vertx, "prefix", UUID.nameUUIDFromBytes(bytes).toString()).join(node -> {
          node.put(key, value, cb -> {
          });
          node.get(key, val -> {
            context.assertEquals(value, val);
            test.complete();
          });
        });
      });
    });
  }

  @Test
  public void multiDhtNodesDoubleKey(TestContext context) {
    Async test = context.async();
    Random rand = new Random(123456);
    Supplier<Double> supply = () -> rand.nextDouble() * 20 - 10;
    Double key = supply.get();
    String value = new JsonObject().put("Hello", "World").encode();

    new DataNode<>(vertx, "prefix", supply.get()).join(n1 -> {
      new DataNode<>(vertx, "prefix", supply.get()).join(n2 -> {
        new DataNode<>(vertx, "prefix", supply.get()).join(node -> {
          node.put(key, value, cb -> {
          });
          node.get(key, val -> {
            context.assertEquals(value, val);
            test.complete();
          });
        });
      });
    });
  }
  
  @Test
  public void multiDhtNodesDoubleKeyCallbackNull(TestContext context) {
    Async test = context.async();
    Random rand = new Random(123456);
    Supplier<Double> supply = () -> rand.nextDouble() * 20 - 10;
    Double key = supply.get();
    String value = new JsonObject().put("Hello", "World").encode();

    new DataNode<>(vertx, "prefix", supply.get()).join(n1 -> {
      new DataNode<>(vertx, "prefix", supply.get()).join(n2 -> {
        new DataNode<>(vertx, "prefix", supply.get()).join(node -> {
          // No callback for put
          node.put(key, value, null);
          node.get(key, val -> {
            context.assertEquals(value, val);
            test.complete();
          });
        });
      });
    });
  }
}
