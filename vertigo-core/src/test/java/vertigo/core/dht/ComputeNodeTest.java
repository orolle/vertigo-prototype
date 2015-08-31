/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package vertigo.core.dht;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.MessageConsumer;
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
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.javatuples.Pair;
import vertigo.core.dht.ComputeNode;
import vertigo.core.dht.DataNode;

/**
 *
 * @author muhaaa
 */
@RunWith(VertxUnitRunner.class)
public class ComputeNodeTest {

  Vertx vertx;

  public ComputeNodeTest() {
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
  public void multiComputeNodes(TestContext context) {
    Async test = context.async();
    Random rand = new Random(123456);
    final String addr = "test.address";
    final Integer key = Integer.MAX_VALUE / 2;
    final String value = "Hello World!";
    
    final int[] i = {2};
    final Consumer<Void> testComplete = v -> {
      i[0] --;
      if(i[0] == 0) {
        test.complete();
      }
    };
    
    MessageConsumer<String> consumer = vertx.eventBus().consumer(addr);
    consumer.handler(msg -> {
      assertEquals(msg.body(), value);
      testComplete.accept(null);
    });
    
    new ComputeNode<Integer>().join(vertx, "prefix", rand.nextInt(), n1 -> {
      new ComputeNode<Integer>().join(vertx, "prefix", rand.nextInt(), n2 -> {
        ComputeNode<Integer> exec = new ComputeNode<>();
        exec.join(vertx, "prefix", rand.nextInt(), node -> {
          exec.execute(rand.nextInt(), dhtNode -> {
            dhtNode.getVertx().eventBus().send(addr, value);
          }, t -> {
            assertNull(t);
            testComplete.accept(null);
          });
        });
      });
    });
  }
}
