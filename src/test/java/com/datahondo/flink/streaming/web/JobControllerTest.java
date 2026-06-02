package com.datahondo.flink.streaming.web;

import com.datahondo.flink.streaming.config.StreamingJobConfig;
import com.datahondo.flink.streaming.job.StreamingJobOrchestrator;
import com.datahondo.flink.streaming.web.model.JobRequest;
import com.datahondo.flink.streaming.web.service.KafkaValidatorService;
import com.datahondo.flink.streaming.web.service.SqlValidatorService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.verify;

import com.datahondo.flink.streaming.audit.AuditEvent;
import com.datahondo.flink.streaming.audit.AuditEventType;
import com.datahondo.flink.streaming.audit.InMemoryAuditCache;
import com.datahondo.flink.streaming.audit.ReconciliationReport;
import org.springframework.http.ResponseEntity;
import org.springframework.http.HttpStatus;
import java.util.Collections;
import java.util.List;

@ExtendWith(MockitoExtension.class)
class JobControllerTest {

    @Mock
    private InMemoryAuditCache auditCache;

    @Mock
    private StreamingJobOrchestrator orchestrator;

    @Mock
    private KafkaValidatorService validatorService;

    @Mock
    private SqlValidatorService sqlValidatorService;
    
    @Mock
    private StreamingJobConfig systemConfig;

    @InjectMocks
    private JobController jobController;

    @Test
    void submitJob_shouldSetDefaultResultTableName_whenMissingInRequest() throws Exception {
        // Given
        JobRequest request = new JobRequest();
        request.setJobName("test-job");
        request.setResultTableName(null); // Missing result table name

        JobRequest.SourceJobRequest source = new JobRequest.SourceJobRequest();
        source.setSourceStartingOffset("EARLIEST");
        request.setSources(java.util.Collections.singletonList(source));

        // When
        jobController.submitJob(request);

        // Then
        ArgumentCaptor<StreamingJobConfig> configCaptor = ArgumentCaptor.forClass(StreamingJobConfig.class);
        verify(orchestrator).submitJob(configCaptor.capture());

        StreamingJobConfig capturedConfig = configCaptor.getValue();
        assertNotNull(capturedConfig.getTransformation());
        assertEquals("result_table", capturedConfig.getTransformation().getResultTableName());
        assertEquals("EARLIEST", capturedConfig.getSources().get(0).getKafka().getStartingOffset());
    }
    
    @Test
    void submitJob_shouldUseProvidedResultTableName_whenPresentInRequest() throws Exception {
        // Given
        JobRequest request = new JobRequest();
        request.setJobName("test-job");
        request.setResultTableName("custom_result_table");
        
        // When
        jobController.submitJob(request);

        // Then
        ArgumentCaptor<StreamingJobConfig> configCaptor = ArgumentCaptor.forClass(StreamingJobConfig.class);
        verify(orchestrator).submitJob(configCaptor.capture());

        StreamingJobConfig capturedConfig = configCaptor.getValue();
        assertNotNull(capturedConfig.getTransformation());
        assertEquals("custom_result_table", capturedConfig.getTransformation().getResultTableName());
    }

    @Test
    void getJobConfig_returnsNotFound_whenFileDoesNotExist() {
        ResponseEntity<?> response = jobController.getJobConfig("non-existent");
        assertEquals(HttpStatus.NOT_FOUND, response.getStatusCode());
    }

    @Test
    void getJobAuditEvents_returnsEventsFromCache() {
        AuditEvent event = AuditEvent.builder().jobName("test-job").eventType(AuditEventType.JOB_RUNNING).build();
        org.mockito.Mockito.when(auditCache.getEvents("test-job")).thenReturn(Collections.singletonList(event));
        
        ResponseEntity<List<AuditEvent>> response = jobController.getJobAuditEvents("test-job");
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(1, response.getBody().size());
    }

    @Test
    void getJobReconciliation_returnsNoContent_whenNoReport() {
        org.mockito.Mockito.when(auditCache.getLatestReport("test-job")).thenReturn(null);
        
        ResponseEntity<ReconciliationReport> response = jobController.getJobReconciliation("test-job");
        assertEquals(HttpStatus.NO_CONTENT, response.getStatusCode());
    }

    @Test
    void getJobReconciliation_returnsReport() {
        ReconciliationReport report = ReconciliationReport.builder().jobName("test-job").build();
        org.mockito.Mockito.when(auditCache.getLatestReport("test-job")).thenReturn(report);
        
        ResponseEntity<ReconciliationReport> response = jobController.getJobReconciliation("test-job");
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(report, response.getBody());
    }
}
