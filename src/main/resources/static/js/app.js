document.addEventListener('DOMContentLoaded', () => {
    const form = document.getElementById('jobForm');
    const submitBtn = document.getElementById('submitBtn');
    const validateBtn = document.getElementById('validateBtn');

    // Invalidate state on input change
    form.addEventListener('input', () => {
        submitBtn.disabled = true;
    });

    validateBtn.addEventListener('click', async () => {
        setLoading(validateBtn, true);
        clearLogs();

        try {
            const data = getFormData();
            addLog('Starting validation request...', 'info');

            const response = await fetch('/api/jobs/validate', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(data)
            });

            const result = await response.json();

            // Render logs from server
            if (result.logs && Array.isArray(result.logs)) {
                result.logs.forEach(log => {
                    let type = 'info';
                    if (log.includes('✅')) type = 'success';
                    if (log.includes('❌')) type = 'error';
                    addLog(log, type);
                });
            }

            if (result.valid) {
                showNotification('Validation successful. You can now deploy.', 'success');
                submitBtn.disabled = false;
                updateValidationStatus(true);
            } else {
                showNotification('Validation failed. Check logs for details.', 'error');
                submitBtn.disabled = true;
                updateValidationStatus(false);
            }
        } catch (error) {
            addLog('Network Error: ' + error.message, 'error');
            showNotification('Validation error: ' + error.message, 'error');
        } finally {
            setLoading(validateBtn, false);
        }
    });

    form.addEventListener('submit', async (e) => {
        e.preventDefault();

        setLoading(submitBtn, true);

        try {
            const data = getFormData();
            showNotification('Submitting job...', 'info');
            addLog('Submitting job deployment...', 'info');

            const response = await fetch('/api/jobs/submit', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(data)
            });

            const result = await response.text();

            if (response.ok) {
                showNotification(result, 'success');
                addLog('Deployment Successful: ' + result, 'success');
                // Ideally reset form or disable submit again to prevent double submit
                submitBtn.disabled = true;
            } else {
                showNotification(result, 'error');
                addLog('Deployment Failed: ' + result, 'error');
            }
        } catch (error) {
            showNotification('Network error: ' + error.message, 'error');
            addLog('Network error: ' + error.message, 'error');
        } finally {
            setLoading(submitBtn, false);
        }
    });

    // Log Panel Functions
    const logContainer = document.getElementById('logContainer');
    const validationStatus = document.getElementById('validationStatus');

    function clearLogs() {
        logContainer.innerHTML = '';
        validationStatus.className = 'indicator';
    }

    function addLog(message, type) {
        // Remove empty state if present
        const emptyState = logContainer.querySelector('.empty-state');
        if (emptyState) {
            emptyState.remove();
        }

        const div = document.createElement('div');
        div.className = `log-item ${type}`;
        div.textContent = `[${new Date().toLocaleTimeString()}] ${message}`;
        logContainer.appendChild(div);

        // Auto scroll
        logContainer.scrollTop = logContainer.scrollHeight;
    }

    function updateValidationStatus(isValid) {
        validationStatus.className = `indicator ${isValid ? 'valid' : 'invalid'}`;
    }

    function getFormData() {
        // Build object manually to handle non-input elements if needed
        const data = {};

        // Inputs
        data.jobName = document.getElementById('jobName').value;
        data.parallelism = parseInt(document.getElementById('parallelism').value);
        data.checkpointInterval = parseInt(document.getElementById('checkpointInterval').value);

        // Source
        data.sourceTopic = document.getElementById('sourceTopic').value;
        data.sourceBootstrapServers = document.getElementById('sourceBootstrapServers').value;
        data.sourceGroupId = document.getElementById('sourceGroupId').value;
        data.sourceGroupId = document.getElementById('sourceGroupId').value;
        data.sourceStartingOffset = document.getElementById('sourceStartingOffset').value;
        const timestampVal = document.getElementById('sourceStartingOffsetTimestamp').value;
        if (timestampVal) {
            data.sourceStartingOffsetTimestamp = parseInt(timestampVal);
        }

        data.sourceTableName = document.getElementById('sourceTableName').value;

        // Source Format
        const sourceFormatEl = document.getElementById('sourceFormat');
        data.sourceFormat = sourceFormatEl ? sourceFormatEl.value : 'STRING';
        data.sourceSchema = document.getElementById('sourceSchema').value; // Add schema

        // Watermark
        const enableWatermarkChk = document.getElementById('enableWatermark');
        data.enableWatermark = enableWatermarkChk ? enableWatermarkChk.checked : false;

        if (data.enableWatermark) {
            const selectedMode = document.querySelector('input[name="watermarkMode"]:checked');
            data.watermarkMode = selectedMode ? selectedMode.value : 'PROCESS_TIME';
            if (data.watermarkMode === 'EXISTING') {
                data.watermarkColumn = document.getElementById('watermarkColumn').value;
            }
        }

        const sourceAuthType = document.getElementById('sourceAuthType').value;
        data.sourceAuthType = sourceAuthType;
        if (sourceAuthType !== 'NONE') {
            data.sourceUsername = document.getElementById('sourceUsername').value;
            data.sourcePassword = document.getElementById('sourcePassword').value;
            data.sourceMechanism = document.getElementById('sourceMechanism').value;
        }

        // Target
        data.targetTopic = document.getElementById('targetTopic').value;
        data.targetBootstrapServers = document.getElementById('targetBootstrapServers').value;

        // Target Format
        const targetFormatEl = document.getElementById('targetFormat');
        data.targetFormat = targetFormatEl ? targetFormatEl.value : 'STRING';
        data.targetSchema = document.getElementById('targetSchema').value;

        const targetAuthType = document.getElementById('targetAuthType').value;
        data.targetAuthType = targetAuthType;
        if (targetAuthType !== 'NONE') {
            data.targetUsername = document.getElementById('targetUsername').value;
            data.targetPassword = document.getElementById('targetPassword').value;
            data.targetMechanism = document.getElementById('targetMechanism').value;
        }

        // Transformation
        data.sqlQuery = document.getElementById('sqlQuery').value;

        return data;
    }

    // Schema File Upload Handler
    const schemaFile = document.getElementById('schemaFile');
    const sourceSchema = document.getElementById('sourceSchema');
    const sourceFormat = document.getElementById('sourceFormat');
    const sourceSchemaLabel = document.getElementById('source-schema-label');

    // Dynamic Schema Label
    if (sourceFormat && sourceSchemaLabel) {
        sourceFormat.addEventListener('change', () => {
            const fmt = sourceFormat.value;
            if (fmt === 'AVRO') {
                sourceSchemaLabel.textContent = 'Data Schema (AVRO Schema):';
                sourceSchema.placeholder = 'e.g. { "type": "record", "name": "User", "fields": [...] }';
            } else {
                sourceSchemaLabel.textContent = 'Data Schema (JSON Schema):';
                sourceSchema.placeholder = 'e.g. { "type": "object", "properties": { "id": {"type": "integer"} } }';
            }
        });
    }

    if (schemaFile) {
        schemaFile.addEventListener('change', (e) => {
            const file = e.target.files[0];
            if (!file) return;

            const reader = new FileReader();
            reader.onload = (e) => {
                sourceSchema.value = e.target.result;
            };
            reader.readAsText(file);
        });
    }

    function setLoading(btn, isLoading) {
        const textSpan = btn.querySelector('.btn-text');
        const loaderDiv = btn.querySelector('.loader');

        if (isLoading) {
            btn.disabled = true;
            if (textSpan) textSpan.classList.add('hidden');
            if (loaderDiv) loaderDiv.classList.remove('hidden');
        } else {
            btn.disabled = false;
            if (textSpan) textSpan.classList.remove('hidden');
            if (loaderDiv) loaderDiv.classList.add('hidden');
        }
    }

    const sourceAuthSelect = document.getElementById('sourceAuthType');
    const targetAuthSelect = document.getElementById('targetAuthType');

    // Auth field toggling
    function toggleAuthFields(typeSelect) {
        const type = typeSelect.value;
        const fields = document.querySelectorAll(`.auth-field[data-for="${typeSelect.id}"]`);

        fields.forEach(field => {
            if (type === 'NONE') {
                field.classList.add('hidden');
            } else {
                field.classList.remove('hidden');
            }
        });
    }

    if (sourceAuthSelect) {
        sourceAuthSelect.addEventListener('change', () => toggleAuthFields(sourceAuthSelect));
    }

    if (targetAuthSelect) {
        targetAuthSelect.addEventListener('change', () => toggleAuthFields(targetAuthSelect));
    }

    // Offset Timestamp Toggle
    const sourceOffsetSelect = document.getElementById('sourceStartingOffset');
    const offsetTimestampGroup = document.getElementById('offsetTimestampGroup');

    if (sourceOffsetSelect) {
        sourceOffsetSelect.addEventListener('change', () => {
            if (sourceOffsetSelect.value === 'TIMESTAMP') {
                offsetTimestampGroup.classList.remove('hidden');
            } else {
                offsetTimestampGroup.classList.add('hidden');
            }
        });
    }

    // Watermark Logic
    const enableWatermark = document.getElementById('enableWatermark');
    const watermarkOptions = document.getElementById('watermarkOptions');
    const watermarkModeRadios = document.getElementsByName('watermarkMode');
    const watermarkColumnGroup = document.getElementById('watermarkColumnGroup');

    if (enableWatermark) {
        enableWatermark.addEventListener('change', () => {
            if (enableWatermark.checked) {
                watermarkOptions.classList.remove('hidden');
            } else {
                watermarkOptions.classList.add('hidden');
            }
        });
    }

    if (watermarkModeRadios) {
        Array.from(watermarkModeRadios).forEach(radio => {
            radio.addEventListener('change', (e) => {
                if (e.target.value === 'EXISTING') {
                    watermarkColumnGroup.classList.remove('hidden');
                } else {
                    watermarkColumnGroup.classList.add('hidden');
                }
            });
        });
    }

    function showNotification(message, type) {
        const notification = document.getElementById('notification');
        const content = notification.querySelector('.notification-content');

        content.textContent = message;
        notification.className = `notification show ${type}`;

        setTimeout(() => {
            notification.classList.remove('show');
        }, 5000);
    }

    // Config Loader
    const configLoader = document.getElementById('configLoader');
    if (configLoader) {
        configLoader.addEventListener('change', (e) => {
            const file = e.target.files[0];
            if (!file) return;

            const reader = new FileReader();
            reader.onload = (e) => {
                try {
                    console.log("Parsing config JSON...");
                    const config = JSON.parse(e.target.result);
                    console.log("Loaded config:", config);

                    // Handle typo from manual edits
                    if (config.traget && !config.target) {
                        console.warn("Found 'traget' typo in JSON, mapping to 'target'");
                        config.target = config.traget;
                    }

                    populateForm(config);
                    showNotification('Configuration loaded successfully', 'success');
                } catch (err) {
                    showNotification('Failed to parse configuration file', 'error');
                    console.error("Config load error:", err);
                } finally {
                    // Reset input so same file can be selected again
                    configLoader.value = '';
                }
            };
            reader.readAsText(file);
        });
    }

    function populateForm(config) {
        // Helper to safe set value
        const setVal = (id, val) => {
            const el = document.getElementById(id);
            if (el && val !== undefined && val !== null) {
                el.value = val;
                // Trigger change for selects to update UI
                el.dispatchEvent(new Event('change'));
            }
        };

        setVal('jobName', config.jobName);
        setVal('parallelism', config.parallelism);
        setVal('checkpointInterval', config.checkpointInterval);

        // Source
        if (config.source) {
            setVal('sourceBootstrapServers', config.source.sourceBootstrapServers);
            setVal('sourceTopic', config.source.sourceTopic);
            setVal('sourceGroupId', config.source.sourceGroupId);
            setVal('sourceTableName', config.source.sourceTableName);
            setVal('sourceSchema', config.source.sourceSchema);
            setVal('sourceFormat', config.source.sourceFormat); // Set format

            setVal('sourceAuthType', config.source.sourceAuthType);

            // Watermark populate
            const enableWatermarkChk = document.getElementById('enableWatermark');
            if (enableWatermarkChk) {
                enableWatermarkChk.checked = config.source.enableWatermark === true;
                enableWatermarkChk.dispatchEvent(new Event('change'));
            }

            if (config.source.enableWatermark) {
                const radios = document.getElementsByName('watermarkMode');
                Array.from(radios).forEach(r => {
                    if (r.value === config.source.watermarkMode) {
                        r.checked = true;
                        r.dispatchEvent(new Event('change'));
                    }
                });
                setVal('watermarkColumn', config.source.watermarkColumn);
            }

            setTimeout(() => {
                setVal('sourceUsername', config.source.sourceUsername);
                setVal('sourcePassword', config.source.sourcePassword);
                setVal('sourceMechanism', config.source.sourceMechanism);
            }, 0);

            setVal('sourceStartingOffset', config.source.sourceStartingOffset);
            setTimeout(() => {
                setVal('sourceStartingOffsetTimestamp', config.source.sourceStartingOffsetTimestamp);
            }, 0);
        }

        // Target
        if (config.target) {
            setVal('targetBootstrapServers', config.target.targetBootstrapServers);
            setVal('targetTopic', config.target.targetTopic);
            setVal('targetAuthType', config.target.targetAuthType);
            setVal('targetAuthType', config.target.targetAuthType);
            setVal('targetFormat', config.target.targetFormat);
            setVal('targetSchema', config.target.targetSchema);

            setTimeout(() => {
                setVal('targetUsername', config.target.targetUsername);
                setVal('targetPassword', config.target.targetPassword);
                setVal('targetMechanism', config.target.targetMechanism);
            }, 0);
        }

        // Transformation
        if (config.transformation) {
            setVal('sqlQuery', config.transformation.sqlQuery);
            setVal('resultTableName', config.transformation.resultTableName);
        }
    }
});
