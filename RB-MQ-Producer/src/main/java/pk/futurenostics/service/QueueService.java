package pk.futurenostics.service;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.core.MessageCreator;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;

import io.micrometer.common.util.StringUtils;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;
import lombok.extern.slf4j.Slf4j;
import pk.futurenostics.dtos.QueueDto;

@Service
@Slf4j
public class QueueService {
	

	@Autowired private ObjectMapper objectMapper;
	@Autowired private JmsTemplate jmsTemplate;
	
     @Value("${queue.document-index}")
	private  String documentIndex;
     
     private static final String DOC_ID = "docId";

	public void publishToDocumentIndex(String docId) {
			try {
				publishDoucmentIdToQueue(documentIndex, docId);
				
				log.info("Successfully sent the document Id {} to the queue to documentIndex queue {} "
						,docId, documentIndex);
				
			}catch(Exception e) {
				log.error("Error occure while sending the document Id {} to documentIndex queue {} "
						,docId, documentIndex);
			}
	}

	private void publishDoucmentIdToQueue(String queueName, String docId) {
		
		JsonNode docIdNode = new TextNode(docId);
		ObjectNode payload = objectMapper.createObjectNode();
		payload.set(DOC_ID, docIdNode);
		QueueDto queueDto = createQueueDto(queueName, payload);
		log.info("Sending the payload {} to the queue {} ", payload, queueName);
		publish(queueDto);
		
		
		
	}

	private QueueDto createQueueDto(String queueName, ObjectNode payload) {
	
		return QueueDto.builder().queueName(queueName).payload(payload).build();
	}
	
	public ResponseEntity<String> publish(QueueDto dto){
		String strMessage = dto.getPayload().toPrettyString();
		String status = "Success";
		
		if(StringUtils.isNotBlank(dto.getQueueName()) && StringUtils.isNotBlank(strMessage)) {
			try {
				
				jmsTemplate.send(
						dto.getQueueName(),
						new MessageCreator() {

							@Override
							public Message createMessage(Session session) throws JMSException {
							Message message	= session.createTextMessage(strMessage);
							log.debug("sending Records to Queue :{} Message: {}",dto.QueueName, message);
								return message;
							}

						});
				
			} catch (Exception e) {
				log.error("Queue Exception occured while send message to the MQ {}", e);
				status = "FAILURE";
			}
			
			return ResponseEntity.status(HttpStatus.OK).body(status);
		}
		else {
			status = "FAILURE";
			return ResponseEntity.status(HttpStatus.BAD_Request).body(status);
		}
		
		
	}

}
