package vertigo.api.function;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public interface Serializer {
  public static final byte[] EMPTY = new byte[] {};

  public static byte[] serialize(Object o) {
    try {
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      ObjectOutputStream out = new ObjectOutputStream(bos);
      out.writeObject(o);

      return bos.toByteArray();
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  @SuppressWarnings("unchecked")
  public static <T> T deserialize(byte[] data) {
    try {
      ByteArrayInputStream bis = new ByteArrayInputStream(data);
      ObjectInputStream in = new ObjectInputStream(bis);

      return (T) in.readObject();
    } catch (Exception e) {
      e.printStackTrace();
      throw new IllegalStateException(e);
    }
  }


}
