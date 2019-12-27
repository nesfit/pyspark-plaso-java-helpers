package tarzan.helpers.py4j;

import java.io.IOException;
import java.io.InputStream;

public class InputStreamReader {
    /**
     * Read data of maximal size from the input stream.
     *
     * @param stream the input stream to read
     * @param count  the maximal number of bytes to read
     * @return resulting byte-array and the actual size read wrapped in ByteArrayContent
     * @throws IOException read error
     */
    public static ByteArrayContent readInputStream(InputStream stream, int count) throws IOException {
        // see https://stackoverflow.com/a/25969355/5265908
        final byte[] bytes = new byte[count];
        final int size = stream.read(bytes);
        return new ByteArrayContent(bytes, size);
    }
}
