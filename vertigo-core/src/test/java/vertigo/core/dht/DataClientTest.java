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
import io.vertx.ext.unit.junit.VertxUnitRunner;
import java.util.Random;
import java.util.UUID;
import java.util.function.Supplier;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 *
 * @author muhaaa
 */
@RunWith(VertxUnitRunner.class)
public class DataClientTest {

  Vertx vertx;

  public DataClientTest() {
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
  public void multiDhtNodes(TestContext context) {
    Async test = context.async();
    Random rand = new Random(123456);
    Integer key = Integer.MAX_VALUE / 2;
    String value = "Hello World!";

    new DataNode<>(vertx, "prefix", rand.nextInt(Integer.MAX_VALUE)).join(n1 -> {
      new DataNode<>(vertx, "prefix", rand.nextInt(Integer.MAX_VALUE)).join(n2 -> {
        new DataNode<>(vertx, "prefix", rand.nextInt(Integer.MAX_VALUE)).join(n3 -> {
          DataClient<Integer, String> client = new DataClient<>(vertx, "prefix");
          
          client.put(key, value, cb -> {
            client.get(key, val -> {
              context.assertEquals(value, val);
              test.complete();
            });
          });
        });
      });
    });
  }
}
