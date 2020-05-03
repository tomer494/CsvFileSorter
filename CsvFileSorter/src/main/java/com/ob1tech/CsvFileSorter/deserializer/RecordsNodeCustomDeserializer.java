package com.ob1tech.CsvFileSorter.deserializer;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.ob1tech.CsvFileSorter.dateModel.RecordBatchNode;
import com.ob1tech.CsvFileSorter.dateModel.RecordIndex;
import com.ob1tech.CsvFileSorter.dateModel.SortKey;

/**
 * RecordBatchNode json deserializer
 * @author Madmon Tomer
 *
 * @param <T>
 * @see AbstractDeserializer
 */
public class RecordsNodeCustomDeserializer<T extends Comparable<T>> extends AbstractDeserializer<T, RecordBatchNode<T>> {
    
    /**
	 * 
	 */
	private static final long serialVersionUID = -4752991084731574549L;

	public RecordsNodeCustomDeserializer(Class<?> vc) {
        super(vc);
    }
 
    public RecordsNodeCustomDeserializer(Class<?> vc, String keyType) {
        super(vc);
        this.keyType = keyType;
    }
 
	@Override
    public RecordBatchNode<T> deserialize(JsonParser parser, DeserializationContext deserializer) {
    	RecordBatchNode<T> recordsNode = null;
        try {
        	ObjectCodec codec = parser.getCodec();
	        JsonNode node = codec.readTree(parser);		         
	        
	        JsonNode vNode = node.get("id");
			Long id = vNode.isNull()?null:vNode.asLong();
			/*
			vNode = node.get("leftNode");
			Long leftNode = vNode.isNull()?null:vNode.asLong();
			
			vNode = node.get("rightNode");
			Long rightNode = vNode.isNull()?null:vNode.asLong();
			*/
			vNode = node.get("key");
			SortKey<T> indexKey = getIndexKeyByType(vNode);
	        
	        JsonNode keyNLineNode = node.get("records");
			List<RecordIndex<T>> records = new LinkedList<RecordIndex<T>>();
			Iterator<JsonNode> elements = keyNLineNode.elements();
			while(elements.hasNext()) {
				JsonNode next = elements.next();
				JsonNode keyNode = next.get("key");
				Comparable<?> key = getByType(keyNode);
				JsonNode recordLineNode = next.get("recordLine");
				long recordLine = recordLineNode.asLong();
				JsonNode recordNode = next.get("record");
				String record = recordNode.asText();
				RecordIndex<T> entry = new RecordIndex<T>(recordLine, (T) key, record);
				records.add((RecordIndex<T>) entry);
			}
			recordsNode = new RecordBatchNode<T>(id);
	    	
			recordsNode.setKey(indexKey);
			/*recordsNode.setLeftNode(leftNode);
			recordsNode.setRightNode(rightNode);*/
	        recordsNode.setRecords(records);
	        
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        return recordsNode;
    }
}