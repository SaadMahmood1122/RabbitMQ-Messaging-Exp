package pk.futurenostics.service;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class QueueService {
	
	private static final String DOCUMENT_ID = "docId";
	
	@Autowired private ObjectMapper mapper;
	// @Autowired private FutherOperationService service;
	
	public void receive(String message) throws IOException {
		if(StringUtils.isBlank(message)) {
			log.info("Search service: Message is Null");
			return;
		}
		
		log.info("Search service: Document event and message is {} ", message);
		
		JsonNode payload = mapper.reader().readValue(message.trim(), JsonNode.class);
		
		if(null == payload || null == payload.get(DOCUMENT_ID)
				|| StringUtils.isNotBlank(payload.get(DOCUMENT_ID).textValue())
				) {
			log.error("Search Service Invalid payload is found: {}",payload);
		}
		
		log.info("Search service: Receive document and doc id is {} ", payload.get(DOCUMENT_ID).textValue());
		//docService.readAndInjest(payload.get(DOCUMENT_ID).textValue());
	}

}
