package org.heigit.bigspatialdata.oshdb.util.function;

import java.io.IOException;

@FunctionalInterface
public interface IOFunction<T,R> {

  R apply(T t) throws IOException;
}
