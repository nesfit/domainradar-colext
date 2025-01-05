package cz.vut.fit.domainradar.serialization;

import com.fasterxml.jackson.core.util.BufferRecycler;
import com.fasterxml.jackson.core.util.ByteArrayBuilder;
import com.fasterxml.jackson.core.util.JsonRecyclerPools;
import com.fasterxml.jackson.core.util.RecyclerPool;
import com.fasterxml.jackson.databind.ObjectMapper;
import cz.vut.fit.domainradar.models.results.CommonIPResult;
import org.apache.kafka.common.errors.SerializationException;

import java.io.IOException;

public class JsonIPResultSerializer<TData> extends JsonSerializer<CommonIPResult<TData>> {
    private final RecyclerPool<BufferRecycler> _pool;

    public JsonIPResultSerializer(ObjectMapper objectMapper) {
        super(objectMapper);
        _pool = JsonRecyclerPools.defaultPool();
    }

    @Override
    public byte[] serialize(String topic, CommonIPResult<TData> data) {
        if (data == null)
            return null;

        final BufferRecycler br = _pool.acquireAndLinkPooled();
        try (ByteArrayBuilder bb = new ByteArrayBuilder()) {
            _objectMapper.writeValue(bb, data);
            final var tag = TagRegistry.TAGS.get(data.collector());
            if (tag == null)
                throw new SerializationException("Unknown IP collector: " + data.collector());
            bb.appendTwoBytes(tag);
            final byte[] result = bb.toByteArray();
            bb.release();
            return result;
        } catch (IOException e) {
            throw new SerializationException(e);
        } finally {
            br.releaseToPool();
        }
    }
}
