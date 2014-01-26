/*
 * Copyright 2012 Last.fm
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package fm.last.moji.impl;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.concurrent.locks.Lock;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import fm.last.moji.tracker.Destination;
import fm.last.moji.tracker.Tracker;
import fm.last.moji.tracker.TrackerException;
import fm.last.moji.tracker.TrackerFactory;

class PutCommand implements MojiCommand {

  private static final Logger log = LoggerFactory.getLogger(PutCommand.class);

  final String key;
  final String domain;
  final String storageClass;
  private final TrackerFactory trackerFactory;
  private final HttpConnectionFactory httpFactory;
  private final Lock writeLock;
  private final byte[] b;

  PutCommand(TrackerFactory trackerFactory, HttpConnectionFactory httpFactory, String key, String domain,
      String storageClass, byte[] b, Lock writeLock) {
    this.trackerFactory = trackerFactory;
    this.httpFactory = httpFactory;
    this.key = key;
    this.domain = domain;
    this.storageClass = storageClass;
    this.b = b;
    this.writeLock = writeLock;
  }

  @Override
  public void executeWithTracker(Tracker tracker) throws IOException {
    final List<Destination> destinations = tracker.createOpen(key, domain, storageClass);
    if (destinations.isEmpty()) {
      throw new TrackerException("Failed to obtain destinations for domain=" + domain + ",key=" + key
          + ",storageClass=" + storageClass);
    }
    IOException lastException = null;
    for (final Destination destination : destinations) {
      log.debug("Creating output stream to: {}", destination);
      OutputStream stream = null;
      try {
        stream = new FileUploadOutputStream(trackerFactory, httpFactory, key, domain, b.length, destinations.get(0), writeLock);
        stream.write(b);
        return;
      } catch (final IOException e) {
        log.debug("Failed to open output -> {}", destination);
        log.debug("Exception was: ", e);
        lastException = e;
      }
      finally {
          IOUtils.closeQuietly(stream);
      }
    }
    throw lastException;
  }

  @Override
  public String toString() {
    final StringBuilder builder = new StringBuilder();
    builder.append("GetOutputStreamCommand [domain=");
    builder.append(domain);
    builder.append(", key=");
    builder.append(key);
    builder.append(", storageClass=");
    builder.append(storageClass);
    builder.append("]");
    return builder.toString();
  }

}
