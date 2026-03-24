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

@ExtendWith(MockitoExtension.class)
class JobControllerTest {

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
}
