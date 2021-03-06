package vertigo.api.function;


import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.javatuples.Pair;
import org.reactivestreams.Processor;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

public class ExecutionContext<C, T, R> implements Processor<T, R>, Serializable {
  private static final long serialVersionUID = -2856282687873376802L;
  private static final byte[] EMPTY = new byte[] {};

  private final byte[] ser;
  private transient SerializableConsumer<R> handleResult;
  private transient List<Subscriber<? super R>> subscribers;

  private transient C contex;
  private transient AsyncFunction<Pair<ExecutionContext<C, T, R>, T>, R> function;

  public ExecutionContext(AsyncFunction<Pair<ExecutionContext<C, T, R>, T>, R> f) {
    this(Serializer.serialize(f));
  }

  public ExecutionContext() {
    this(Serializer.EMPTY);
  }

  public ExecutionContext(byte[] ser) {
    this.ser = ser;
    init();
  }
  
  private void init() {
    if (function == null && ser != EMPTY) {
      function = Serializer.deserialize(ser);
      handleResult = (R r) -> handleResult(r);
      subscribers = new ArrayList<>();
    }
  }

  public ExecutionContext<C, T, R> context(C context) {
    this.contex = context;
    return this;
  }

  public C context() {
    return contex;
  }

  public byte[] serialize() {
    return ser;
  }

  private void handleResult(R r) {
    init();

    subscribers.forEach(s -> s.onNext(r));
  }

  @Override
  public void onSubscribe(Subscription s) {
    s.request(Long.MAX_VALUE);
  }

  @Override
  public void onNext(T t) {
    init();

    Pair<ExecutionContext<C, T, R>, T> pair = new Pair<>(this, t);
    function.apply(pair, handleResult);
  }

  @Override
  public void onError(Throwable t) {
    init();

    subscribers.forEach(s -> s.onError(t));
  }

  @Override
  public void onComplete() {
    init();

    subscribers.forEach(s -> s.onComplete());
  }

  @Override
  public void subscribe(Subscriber<? super R> s) {
    init();

    subscribers.add(s);
  }

}
