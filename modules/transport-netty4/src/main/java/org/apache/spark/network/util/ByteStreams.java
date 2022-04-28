/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.apache.spark.network.util;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

public class ByteStreams {
    private static final int BUFFER_SIZE = 8192;

    public static void skipFully(InputStream in, long n) throws IOException {
        long skipped = skipUpTo(in, n);
        if (skipped < n) {
            throw new EOFException("reached end of stream after skipping " + skipped + " bytes; " + n + " bytes expected");
        }
    }

    /**
     * Discards up to {@code n} bytes of data from the input stream. This method will block until
     * either the full amount has been skipped or until the end of the stream is reached, whichever
     * happens first. Returns the total number of bytes skipped.
     */
    static long skipUpTo(InputStream in, long n) throws IOException {
        long totalSkipped = 0;
        // A buffer is allocated if skipSafely does not skip any bytes.
        byte[] buf = null;

        while (totalSkipped < n) {
            long remaining = n - totalSkipped;
            long skipped = skipSafely(in, remaining);

            if (skipped == 0) {
                // Do a buffered read since skipSafely could return 0 repeatedly, for example if
                // in.available() always returns 0 (the default).
                int skip = (int) Math.min(remaining, BUFFER_SIZE);
                if (buf == null) {
                    // Allocate a buffer bounded by the maximum size that can be requested, for
                    // example an array of BUFFER_SIZE is unnecessary when the value of remaining
                    // is smaller.
                    buf = new byte[skip];
                }
                if ((skipped = in.read(buf, 0, skip)) == -1) {
                    // Reached EOF
                    break;
                }
            }

            totalSkipped += skipped;
        }

        return totalSkipped;
    }

    /**
     * Attempts to skip up to {@code n} bytes from the given input stream, but not more than {@code
     * in.available()} bytes. This prevents {@code FileInputStream} from skipping more bytes than
     * actually remain in the file, something that it {@linkplain java.io.FileInputStream#skip(long)
     * specifies} it can do in its Javadoc despite the fact that it is violating the contract of
     * {@code InputStream.skip()}.
     */
    private static long skipSafely(InputStream in, long n) throws IOException {
        int available = in.available();
        return available == 0 ? 0 : in.skip(Math.min(available, n));
    }
}
