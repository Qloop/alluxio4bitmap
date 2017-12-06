/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client.file;

import java.io.Closeable;
import java.io.IOException;

/**
 * input interface instead of {@link java.io.InputStream}
 * <p>
 * corresponding to {@link Output}
 */
public interface Input extends Closeable {

  /**
   * read next 1 byte to int
   */
  int readByte() throws IOException;

  /**
   * read next 1 byte to boolean
   */
  boolean readBool() throws IOException;

  /**
   * read next 2 bytes to int
   */
  int readShort() throws IOException;

  /**
   * read next 4 bytes to int
   */
  int readInt() throws IOException;

  /**
   * read next 4 bytes to float
   */
  float readFloat() throws IOException;

  /**
   * read next 8 bytes to long
   */
  long readLong() throws IOException;

  /**
   * read next 8 bytes to double
   */
  double readDouble() throws IOException;

  /**
   * read next 4 bytes to int as the length of the string,
   * then read next length bytes to string
   */
  String readString() throws IOException;


  /**
   * close the input
   */
  void close() throws IOException;


  /**
   * read 1 byte to int at specific position
   */
  int readByte(int pos) throws IOException;

  /**
   * read specific position 1 byte to boolean
   */
  boolean readBool(int pos) throws IOException;

  /**
   * read 2 bytes to int at specific position
   */
  int readShort(int pos) throws IOException;

  /**
   * read 4 bytes to int at specific position
   */
  int readInt(int pos) throws IOException;

  /**
   * read  4 bytes to float at specific position
   */
  float readFloat(int pos) throws IOException;

  /**
   * read  8 bytes to long at specific position
   */
  long readLong(int pos) throws IOException;

  /**
   * read  8 bytes to double at specific position
   */
  double readDouble(int pos) throws IOException;


  /**
   * read specific position 4 bytes to int as the length of the string,
   * then read next length bytes to string
   */
  String readString(int pos) throws IOException;
}
