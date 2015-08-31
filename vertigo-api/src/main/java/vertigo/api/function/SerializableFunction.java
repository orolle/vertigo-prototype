/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package vertigo.api.function;

import java.io.Serializable;
import java.util.function.Function;

/**
 *
 * @author muhaaa
 */
public interface SerializableFunction<T,R> extends Function<T, R>, Serializable {
  
}
