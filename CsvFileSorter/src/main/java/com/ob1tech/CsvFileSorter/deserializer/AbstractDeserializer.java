package com.ob1tech.CsvFileSorter.deserializer;

import org.apache.commons.lang3.math.NumberUtils;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.ob1tech.CsvFileSorter.dateModel.IndexNode;
import com.ob1tech.CsvFileSorter.dateModel.SortKey;

public abstract class AbstractDeserializer<T, N> extends StdDeserializer<N> {
    
    /**
	 * 
	 */
	private static final long serialVersionUID = 901250332340210404L;
	
	String keyType;

	/*public String getKeyType() {
		return keyType;
	}

	public void setKeyType(String keyType) {
		this.keyType = keyType;
	}*/

	public AbstractDeserializer(Class<?> vc) {
        super(vc);
    }
 
    public AbstractDeserializer(Class<?> vc, String keyType) {
        super(vc);
        this.keyType = keyType;
    }
 
	@Override
    public abstract N deserialize(JsonParser parser, DeserializationContext deserializer);

	@SuppressWarnings({ "unchecked", "rawtypes" })
	protected SortKey<T> getIndexKeyByType(JsonNode node) {

		JsonNode vMaxNode = node.get("maxValue");
		JsonNode vMinNode = node.get("minValue");
		SortKey ik;
		if(String.class.getTypeName().equals(keyType)) {
			ik = new SortKey<String>(vMinNode.asText(), vMaxNode.asText());
			return ik;
		}
		if(Double.class.getTypeName().equals(keyType)) {
			ik = new SortKey<Double>(vMinNode.asDouble(), vMaxNode.asDouble());
			return ik;
		}
		ik = new SortKey<Long>(vMinNode.asLong(), vMaxNode.asLong());
		return ik;
	}

	public Comparable<?> getByType(JsonNode node) {
		if(String.class.getTypeName().equals(keyType)) {
			return node.asText();
		}
		if(Double.class.getTypeName().equals(keyType)) {
			return node.asDouble();
		}
		return node.asLong();
	}
}