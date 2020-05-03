package com.ob1tech.CsvFileSorter.deserializer;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.ob1tech.CsvFileSorter.dateModel.IndexNode;
import com.ob1tech.CsvFileSorter.dateModel.SortKey;

public class IndexNodeCustomDeserializer<T extends Comparable<T>> extends AbstractDeserializer<T, IndexNode<T>> {
    
    /**
	 * 
	 */
	private static final long serialVersionUID = 7303630346789974423L;

	public IndexNodeCustomDeserializer(Class<?> vc) {
        super(vc);
    }
 
    public IndexNodeCustomDeserializer(Class<?> vc, String keyType) {
        super(vc);
        this.keyType = keyType;
    }
 
	@Override
    public IndexNode<T> deserialize(JsonParser parser, DeserializationContext deserializer) {
    	IndexNode<T> indexNode = null;
        try {
        	ObjectCodec codec = parser.getCodec();
	        JsonNode node = codec.readTree(parser);		         
	        
	        JsonNode vNode = node.get("id");
			Long id = vNode.isNull()?null:vNode.asLong();
			
			vNode = node.get("leftNode");
			Long leftNode = vNode.isNull()?null:vNode.asLong();
			
			vNode = node.get("rightNode");
			Long rightNode = vNode.isNull()?null:vNode.asLong();
			
			vNode = node.get("key");
			SortKey<T> indexKey = getIndexKeyByType(vNode);
	        
	        indexNode = new IndexNode<T>(id, indexKey, leftNode, rightNode);
	        
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        return indexNode;
    }
}