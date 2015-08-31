/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package vertigo.stream;

import vertigo.stream.SimpleReactNode;
import com.aol.simple.react.async.Queue;
import com.aol.simple.react.async.factories.QueueFactories;
import com.aol.simple.react.stream.lazy.LazyFutureStreamImpl;
import com.aol.simple.react.stream.lazy.LazyReact;
import com.aol.simple.react.stream.traits.LazyFutureStream;
import io.netty.util.concurrent.CompleteFuture;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
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
import java.util.Spliterator;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.StreamSupport;

/**
 *
 * @author muhaaa
 */
@RunWith(VertxUnitRunner.class)
public class SimpleReactTest {

  Vertx vertx;

  public SimpleReactTest() {
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
  public void simpleReactOnVertx(TestContext context) {
    Async test = context.async();
    //Vertx vertx = Vertx.vertx();
    String fromAddress = "processing.in";
    String toAddress = "processing.out";
    EventBus eb = vertx.eventBus();
    Object result = new Object();

    Queue<Integer> inQueue = QueueFactories.<Integer>unboundedQueue().build();
    LazyReact react = new LazyReact().
      withAsync(false);

    LazyFutureStream<Integer> processing = react.fromStream(inQueue.streamCompletableFutures());

    processing.
      map(number -> number.toString()).
      peek(str -> eb.send(toAddress, str)).
      run();

    eb.<Integer>consumer(fromAddress).handler(msg -> {
      inQueue.offer(msg.body());
    });

    eb.consumer(toAddress).handler(msg -> {
      if ("3".equals(msg.body())) {
        test.complete();
      }
    });

    eb.send(fromAddress, 1);
    eb.send(fromAddress, 2);
    eb.send(fromAddress, 3);
  }

  @Test
  public void multiSimpleReactNodes(TestContext context) {
    Async test = context.async();
    Random rand = new Random(123456);
    final String addrIn = "processing.in";
    final String addrOut = "processing.out";

    vertx.eventBus().consumer(addrOut).handler(msg -> {
      if ("3".equals(msg.body())) {
        test.complete();
      }
    });

    new SimpleReactNode<Integer>().join(vertx, "prefix", rand.nextInt(), n1 -> {
      new SimpleReactNode<Integer>().join(vertx, "prefix", rand.nextInt(), n2 -> {
        SimpleReactNode<Integer> exec = new SimpleReactNode<>();
        exec.join(vertx, "prefix", rand.nextInt(), node -> {
          exec.<Integer>fromEventbus(rand.nextInt(), addrIn, addrOut, (stream) -> {
            return stream.map(i -> i.toString());
          }, f -> {
            assertTrue(f.succeeded());
            
            vertx.eventBus().send(addrIn, 1);
            vertx.eventBus().send(addrIn, 2);
            vertx.eventBus().send(addrIn, 3);
          });
        });
      });
    });
  }
}
