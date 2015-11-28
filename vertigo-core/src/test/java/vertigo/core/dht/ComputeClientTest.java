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
import io.vertx.ext.unit.junit.VertxUnitRunner;
import java.util.Random;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 *
 * @author muhaaa
 */
@RunWith(VertxUnitRunner.class)
public class ComputeClientTest {

  Vertx vertx;

  public ComputeClientTest() {
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
  public void testTest(TestContext context) {
    Async test = context.async();
    test.complete();
  }

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
    
    new ComputeNode<Integer, Integer>(vertx, "prefix", rand.nextInt()).join(n1 -> {
      new ComputeNode<Integer, Integer>(vertx, "prefix", rand.nextInt()).join(n2 -> {
        new ComputeNode<>(vertx, "prefix", rand.nextInt()).join(node -> {
          ComputeClient<Integer, Integer> client = new ComputeClient<>(vertx, "prefix");
          
          client.execute(rand.nextInt(), dhtNode -> {
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
