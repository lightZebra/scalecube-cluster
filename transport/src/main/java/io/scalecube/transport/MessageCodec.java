package io.scalecube.transport;

import static com.fasterxml.jackson.core.JsonToken.VALUE_NULL;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MappingJsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.EncoderException;
import io.scalecube.transport.Message.Builder;
import java.io.InputStream;
import java.io.OutputStream;

/** Contains static methods for message serializing/deserializing logic. */
public final class MessageCodec {

  private static final ObjectMapper mapper = initMapper();
  private static final MappingJsonFactory jsonFactory = new MappingJsonFactory(mapper);

  /**
   * Deserializes message from given byte buffer.
   *
   * @param bb byte buffer
   * @return message from ByteBuf
   */
  public static Message deserialize3(ByteBuf bb) {
    try (ByteBufInputStream stream = new ByteBufInputStream(bb, true)) {
      return mapper.readValue((InputStream) stream, Message.class);
    } catch (Exception e) {
      //      ReferenceCountUtil.safeRelease(bb);//todo ?
      throw new DecoderException(e.getMessage(), e);
    }
  }

  public static Message deserialize(ByteBuf bb) {
    try (ByteBufInputStream stream = new ByteBufInputStream(bb, true)) {
      JsonParser jp = jsonFactory.createParser((InputStream) stream);

      Builder builder = Message.builder();
      JsonToken current = jp.nextToken();

      if (current != JsonToken.START_OBJECT) {
        throw new DecoderException("Root should be object", null);
      }

      while ((jp.nextToken()) != JsonToken.END_OBJECT) {
        String fieldName = jp.getCurrentName();
        current = jp.nextToken();
        if (current == VALUE_NULL) {
          continue;
        }

        if (fieldName.equals("data")) {
          builder.data(jp.readValueAs(byte[].class));

        } else if (fieldName.equals("sender")) {
          builder.sender(jp.readValueAs(Address.class));
        } else {
          // headers
          builder.header(fieldName, jp.getValueAsString());
        }
      }
      return builder.build();
    } catch (Exception e) {
      //      ReferenceCountUtil.safeRelease(bb);//todo ?
      throw new DecoderException(e.getMessage(), e);
    }
  }

  public static <T> T deserializeData(Object data, Class<T> aClass) {

    try {
      if (data instanceof byte[]) {
        return mapper.readValue((byte[]) data, aClass);
      }
    } catch (Exception e) {
      throw new DecoderException(e.getMessage(), e);
    }
    throw new DecoderException("not completable type: " + data.getClass());
  }

  /**
   * Serializes given message into byte buffer.
   *
   * @param message message to serialize
   * @return message as ByteBuf
   */
  public static ByteBuf serialize(Message message) {
    ByteBuf bb = ByteBufAllocator.DEFAULT.buffer();
    ByteBufOutputStream stream = new ByteBufOutputStream(bb);
    try {
      mapper.writeValue((OutputStream) stream, message);
    } catch (Exception e) {
      bb.release();
      throw new EncoderException(e.getMessage(), e);
    }
    return bb;
  }

  private static ObjectMapper initMapper() {
    ObjectMapper mapper = new ObjectMapper();
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    mapper.configure(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL, true);
    mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
    mapper.setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.ANY);
    mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    mapper.configure(SerializationFeature.WRITE_ENUMS_USING_TO_STRING, true);
    //    mapper.enableDefaultTyping(DefaultTyping.JAVA_LANG_OBJECT, JsonTypeInfo.As.PROPERTY);
    return mapper;
  }
}
