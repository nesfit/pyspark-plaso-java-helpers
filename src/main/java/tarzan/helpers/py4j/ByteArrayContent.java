package tarzan.helpers.py4j;

import java.util.Arrays;
import java.util.Objects;

public class ByteArrayContent {
    private byte[] bytes;
    private int size;

    /**
     * Creates a new byte-array content wrapping the given byte-array and noticing its actual size.
     *
     * @param bytes the byte-array to wrap
     * @param size  the actual size of the byte-array (may differ from its allocation size)
     */
    public ByteArrayContent(byte[] bytes, int size) {
        this.bytes = bytes;
        this.size = size;
    }

    /**
     * Get the byte-array content, see https://www.py4j.org/advanced_topics.html#byte-array-byte.
     *
     * @return the byte-array content
     */
    public byte[] getBytes() {
        return bytes;
    }

    /**
     * Get the actual size of the byte-array content (an allocation size of the byte array can be greater that its actual size).
     *
     * @return the actual size of the byte-array content
     */
    public int getSize() {
        return size;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ByteArrayContent that = (ByteArrayContent) o;
        return size == that.size &&
                Arrays.equals(bytes, that.bytes);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(size);
        result = 31 * result + Arrays.hashCode(bytes);
        return result;
    }
}
