document.addEventListener('DOMContentLoaded', () => {
    const form = document.getElementById('jobForm');
    const submitBtn = document.getElementById('submitBtn');
    const validateBtn = document.getElementById('validateBtn');
    const sourcesContainer = document.getElementById('sourcesContainer');
    const addSourceBtn = document.getElementById('addSourceBtn');
    const sourceTemplate = document.getElementById('sourceTemplate');

    // Invalidate state on input change
    form.addEventListener('input', () => {
        submitBtn.disabled = true;
    });

    // --- Dynamic Source Logic ---

    function addSource(data = null) {
        const index = sourcesContainer.children.length;
        const clone = sourceTemplate.content.cloneNode(true);
        const sourceEntry = clone.querySelector('.source-entry');

        // Update Title
        sourceEntry.querySelector('.source-title').textContent = `Source #${index + 1}`;

        // Setup Event Listeners for this source entry
        setupSourceListeners(sourceEntry);

        // Remove Button Logic
        const removeBtn = sourceEntry.querySelector('.remove-source-btn');
        if (index === 0) {
            removeBtn.style.display = 'none'; // Cannot remove first source
        } else {
            removeBtn.addEventListener('click', () => {
                sourceEntry.remove();
                updateSourceTitles();
                submitBtn.disabled = true;
            });
        }

        // Populate Data if provided
        if (data) {
            populateSourceEntry(sourceEntry, data);
        }

        sourcesContainer.appendChild(sourceEntry);
    }

    function updateSourceTitles() {
        Array.from(sourcesContainer.children).forEach((entry, idx) => {
            entry.querySelector('.source-title').textContent = `Source #${idx + 1}`;
        });
    }

    function setupSourceListeners(entry) {
        // Auth Toggle — show all auth-fields when not NONE, show ssl-fields only for SASL_SSL
        const authSelect = entry.querySelector('select[name="sourceAuthType"]');
        const authFields = entry.querySelectorAll('.auth-field[data-for="sourceAuthType"]');
        const sslFields = entry.querySelectorAll('.ssl-field[data-for="sourceAuthType"]');
        authSelect.addEventListener('change', () => {
            const type = authSelect.value;
            const isAuth = type !== 'NONE';
            authFields.forEach(f => f.classList.toggle('hidden', !isAuth));
            sslFields.forEach(f => f.classList.toggle('hidden', type !== 'SASL_SSL'));
        });

        // Startup Mode Timestamp Toggle
        const startupModeSelect = entry.querySelector('select[name="sourceStartupMode"]');
        const timestampGroup = entry.querySelector('.offset-timestamp-group');
        startupModeSelect.addEventListener('change', () => {
            timestampGroup.classList.toggle('hidden', startupModeSelect.value !== 'timestamp');
        });

        // Schema Type Toggle
        const schemaTypeSelect = entry.querySelector('select[name="sourceSchemaType"]');
        const schemaDefinitionGroup = entry.querySelector('.source-schema-definition-group');
        const schemaRegistryGroups = entry.querySelectorAll('.source-schema-registry-group');
        schemaTypeSelect.addEventListener('change', () => {
            const v = schemaTypeSelect.value;
            schemaDefinitionGroup.classList.toggle('hidden', !v || v === 'REGISTRY');
            schemaRegistryGroups.forEach(g => g.classList.toggle('hidden', v !== 'REGISTRY'));
        });

        // Watermark Toggle
        const wmCheckbox = entry.querySelector('input[name="enableWatermark"]');
        const wmOptions = entry.querySelector('.watermark-options');
        wmCheckbox.addEventListener('change', () => {
            wmOptions.classList.toggle('hidden', !wmCheckbox.checked);
        });

        // Watermark Mode
        const wmRadios = entry.querySelectorAll('input[name="watermarkMode"]');
        const wmColGroup = entry.querySelector('.watermark-column-group');
        wmRadios.forEach(radio => {
            radio.addEventListener('change', () => {
                if (radio.checked) {
                    wmColGroup.classList.toggle('hidden', radio.value !== 'EXISTING');
                }
            });
        });

        // Format Label Change — update schema label text based on selected format
        const formatSelect = entry.querySelector('select[name="sourceFormat"]');
        const schemaLabel = entry.querySelector('.source-schema-label');

        formatSelect.addEventListener('change', () => {
            if (!schemaLabel) return;
            if (formatSelect.value === 'AVRO') {
                schemaLabel.textContent = 'Schema Definition (AVRO):';
            } else {
                schemaLabel.textContent = 'Schema Definition';
            }
        });
    }

    function populateSourceEntry(entry, data) {
        entry.querySelector('input[name="sourceBootstrapServers"]').value = data.sourceBootstrapServers || '';
        entry.querySelector('input[name="sourceTopic"]').value = data.sourceTopic || '';
        entry.querySelector('input[name="sourceGroupId"]').value = data.sourceGroupId || '';

        // Startup mode — accept both new flink-native values and legacy uppercase values
        const startupModeEl = entry.querySelector('select[name="sourceStartupMode"]');
        if (data.sourceStartupMode) {
            startupModeEl.value = data.sourceStartupMode;
        } else if (data.sourceStartingOffset) {
            // Map legacy values to flink-native
            const legacyMap = {
                'EARLIEST': 'earliest-offset',
                'LATEST': 'latest-offset',
                'GROUP_OFFSETS': 'group-offsets',
                'TIMESTAMP': 'timestamp'
            };
            startupModeEl.value = legacyMap[data.sourceStartingOffset] || 'earliest-offset';
        }

        entry.querySelector('input[name="sourceStartingOffsetTimestamp"]').value = data.sourceStartingOffsetTimestamp || '';
        entry.querySelector('input[name="sourceTableName"]').value = data.sourceTableName || '';
        entry.querySelector('input[name="sourceAlias"]').value = data.sourceAlias || '';

        // Schema type and related fields
        const schemaTypeEl = entry.querySelector('select[name="sourceSchemaType"]');
        schemaTypeEl.value = data.sourceSchemaType || '';
        const schemaTextarea = entry.querySelector('textarea[name="sourceSchema"]');
        if (schemaTextarea) schemaTextarea.value = data.sourceSchema || '';
        entry.querySelector('input[name="sourceSchemaRegistryUrl"]').value = data.sourceSchemaRegistryUrl || '';
        entry.querySelector('input[name="sourceSchemaSubject"]').value = data.sourceSchemaSubject || '';

        entry.querySelector('select[name="sourceAuthType"]').value = data.sourceAuthType || 'NONE';
        entry.querySelector('select[name="sourceMechanism"]').value = data.sourceMechanism || 'PLAIN';
        entry.querySelector('input[name="sourceUsername"]').value = data.sourceUsername || '';
        entry.querySelector('input[name="sourcePassword"]').value = data.sourcePassword || '';
        entry.querySelector('input[name="sourceTruststoreLocation"]').value = data.sourceTruststoreLocation || '';
        entry.querySelector('input[name="sourceTruststorePassword"]').value = data.sourceTruststorePassword || '';
        entry.querySelector('input[name="sourceJaasConfig"]').value = data.sourceJaasConfig || '';
        entry.querySelector('select[name="sourceFormat"]').value = data.sourceFormat || 'STRING';

        if (data.enableWatermark) {
            entry.querySelector('input[name="enableWatermark"]').checked = true;
            const wmRadios = entry.querySelectorAll('input[name="watermarkMode"]');
            wmRadios.forEach(r => { if (r.value === data.watermarkMode) r.checked = true; });
            entry.querySelector('input[name="watermarkColumn"]').value = data.watermarkColumn || '';
            const outOfOrderEl = entry.querySelector('input[name="watermarkMaxOutOfOrderness"]');
            if (outOfOrderEl) outOfOrderEl.value = data.watermarkMaxOutOfOrderness !== undefined ? data.watermarkMaxOutOfOrderness : 5000;
        }

        // Trigger change events to update UI visibility
        entry.querySelector('select[name="sourceAuthType"]').dispatchEvent(new Event('change'));
        entry.querySelector('select[name="sourceStartupMode"]').dispatchEvent(new Event('change'));
        entry.querySelector('select[name="sourceSchemaType"]').dispatchEvent(new Event('change'));
        entry.querySelector('input[name="enableWatermark"]').dispatchEvent(new Event('change'));
        entry.querySelector('select[name="sourceFormat"]').dispatchEvent(new Event('change'));
        entry.querySelectorAll('input[name="watermarkMode"]').forEach(r => { if (r.checked) r.dispatchEvent(new Event('change')); });
    }

    addSourceBtn.addEventListener('click', () => addSource());

    // Initialize with one source
    addSource();

    // --- End Dynamic Source Logic ---

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
        const emptyState = logContainer.querySelector('.empty-state');
        if (emptyState) emptyState.remove();

        const div = document.createElement('div');
        div.className = `log-item ${type}`;
        div.textContent = `[${new Date().toLocaleTimeString()}] ${message}`;
        logContainer.appendChild(div);
        logContainer.scrollTop = logContainer.scrollHeight;
    }

    function updateValidationStatus(isValid) {
        validationStatus.className = `indicator ${isValid ? 'valid' : 'invalid'}`;
    }

    function getFormData() {
        const data = {};

        // Inputs
        data.jobName = document.getElementById('jobName').value;
        data.parallelism = parseInt(document.getElementById('parallelism').value);
        data.checkpointInterval = parseInt(document.getElementById('checkpointInterval').value);
        data.checkpointDir = document.getElementById('checkpointDir').value || null;

        // Savepoint restore (optional)
        const spPath = document.getElementById('savepointPath').value.trim();
        if (spPath) {
            data.savepointPath = spPath;
            data.allowNonRestoredState =
                document.getElementById('allowNonRestoredState').checked;
        }

        // Sources
        data.sources = [];
        const sourceEntries = sourcesContainer.querySelectorAll('.source-entry');
        sourceEntries.forEach(entry => {
            const src = {};
            src.sourceBootstrapServers = entry.querySelector('input[name="sourceBootstrapServers"]').value;
            src.sourceTopic = entry.querySelector('input[name="sourceTopic"]').value;
            src.sourceGroupId = entry.querySelector('input[name="sourceGroupId"]').value;

            // Startup mode (flink-native values) + backward-compat alias
            const startupMode = entry.querySelector('select[name="sourceStartupMode"]').value;
            src.sourceStartupMode = startupMode;
            // Map to legacy sourceStartingOffset for backward compatibility
            const startupToLegacy = {
                'earliest-offset': 'EARLIEST',
                'latest-offset': 'LATEST',
                'group-offsets': 'GROUP_OFFSETS',
                'timestamp': 'TIMESTAMP'
            };
            src.sourceStartingOffset = startupToLegacy[startupMode] || 'EARLIEST';

            const ts = entry.querySelector('input[name="sourceStartingOffsetTimestamp"]').value;
            if (ts) src.sourceStartingOffsetTimestamp = parseInt(ts);

            src.sourceTableName = entry.querySelector('input[name="sourceTableName"]').value;
            src.sourceAlias = entry.querySelector('input[name="sourceAlias"]').value;

            // Schema fields
            src.sourceSchemaType = entry.querySelector('select[name="sourceSchemaType"]').value;
            const schemaTextarea = entry.querySelector('textarea[name="sourceSchema"]');
            if (schemaTextarea) src.sourceSchema = schemaTextarea.value;
            src.sourceSchemaRegistryUrl = entry.querySelector('input[name="sourceSchemaRegistryUrl"]').value;
            src.sourceSchemaSubject = entry.querySelector('input[name="sourceSchemaSubject"]').value;

            src.enableWatermark = entry.querySelector('input[name="enableWatermark"]').checked;
            if (src.enableWatermark) {
                const checkedMode = entry.querySelector('input[name="watermarkMode"]:checked');
                src.watermarkMode = checkedMode ? checkedMode.value : 'PROCESS_TIME';
                if (src.watermarkMode === 'EXISTING') {
                    src.watermarkColumn = entry.querySelector('input[name="watermarkColumn"]').value;
                    const outOfOrderEl = entry.querySelector('input[name="watermarkMaxOutOfOrderness"]');
                    if (outOfOrderEl && outOfOrderEl.value !== '') {
                        src.watermarkMaxOutOfOrderness = parseInt(outOfOrderEl.value);
                    }
                }
            } else {
                src.watermarkMode = 'PROCESS_TIME'; // Default fallback
            }

            src.sourceAuthType = entry.querySelector('select[name="sourceAuthType"]').value;
            if (src.sourceAuthType !== 'NONE') {
                src.sourceMechanism = entry.querySelector('select[name="sourceMechanism"]').value;
                src.sourceUsername = entry.querySelector('input[name="sourceUsername"]').value;
                src.sourcePassword = entry.querySelector('input[name="sourcePassword"]').value;
            }
            if (src.sourceAuthType === 'SASL_SSL') {
                src.sourceTruststoreLocation = entry.querySelector('input[name="sourceTruststoreLocation"]').value;
                src.sourceTruststorePassword = entry.querySelector('input[name="sourceTruststorePassword"]').value;
                src.sourceJaasConfig = entry.querySelector('input[name="sourceJaasConfig"]').value;
            }
            src.sourceFormat = entry.querySelector('select[name="sourceFormat"]').value;

            data.sources.push(src);
        });

        // Target
        data.targetType = document.getElementById('targetType').value;
        data.targetTopic = document.getElementById('targetTopic').value;
        data.targetBootstrapServers = document.getElementById('targetBootstrapServers').value;

        // Target Format
        const targetFormatEl = document.getElementById('targetFormat');
        data.targetFormat = targetFormatEl ? targetFormatEl.value : 'STRING';

        // Target Schema
        data.targetSchemaType = document.getElementById('targetSchemaType').value;
        const targetSchemaEl = document.getElementById('targetSchema');
        if (targetSchemaEl) data.targetSchema = targetSchemaEl.value;
        data.targetSchemaRegistryUrl = document.getElementById('targetSchemaRegistryUrl').value;
        data.targetSchemaSubject = document.getElementById('targetSchemaSubject').value;

        const targetAuthType = document.getElementById('targetAuthType').value;
        data.targetAuthType = targetAuthType;
        if (targetAuthType !== 'NONE') {
            data.targetUsername = document.getElementById('targetUsername').value;
            data.targetPassword = document.getElementById('targetPassword').value;
            data.targetMechanism = document.getElementById('targetMechanism').value;
        }
        if (targetAuthType === 'SASL_SSL') {
            data.targetTruststoreLocation = document.getElementById('targetTruststoreLocation').value;
            data.targetTruststorePassword = document.getElementById('targetTruststorePassword').value;
            data.targetJaasConfig = document.getElementById('targetJaasConfig').value;
        }

        // Transformation
        const transformationTypeEl = document.querySelector('input[name="transformationType"]:checked');
        data.transformationType = transformationTypeEl ? transformationTypeEl.value : 'INLINE';

        if (data.transformationType === 'INLINE') {
            data.sqlQuery = document.getElementById('sqlQuery').value;
        } else {
            data.sqlFilePath = document.getElementById('sqlFilePath').value;
        }

        data.resultTableName = document.getElementById('resultTableName').value;

        return data;
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

    // Auth Toggle for TARGET
    const targetAuthSelect = document.getElementById('targetAuthType');
    if (targetAuthSelect) {
        targetAuthSelect.addEventListener('change', () => {
            const type = targetAuthSelect.value;
            const fields = document.querySelectorAll('.auth-field[data-for="targetAuthType"]');
            fields.forEach(field => {
                field.classList.toggle('hidden', type === 'NONE');
            });
            // Show SSL truststore only for SASL_SSL
            const sslGroup = document.getElementById('targetSslGroup');
            if (sslGroup) sslGroup.classList.toggle('hidden', type !== 'SASL_SSL');
            // Also hide truststore password and jaas if not SASL_SSL
            const targetTruststorePasswordEl = document.getElementById('targetTruststorePassword');
            const targetJaasConfigEl = document.getElementById('targetJaasConfig');
            if (targetTruststorePasswordEl) {
                targetTruststorePasswordEl.closest('.form-group').classList.toggle('hidden', type !== 'SASL_SSL');
            }
            if (targetJaasConfigEl) {
                targetJaasConfigEl.closest('.form-group').classList.toggle('hidden', type !== 'SASL_SSL');
            }
        });
    }

    // Target Schema Type Toggle
    const targetSchemaType = document.getElementById('targetSchemaType');
    if (targetSchemaType) {
        targetSchemaType.addEventListener('change', () => {
            const v = targetSchemaType.value;
            document.getElementById('targetSchemaDefinitionGroup').classList.toggle('hidden', !v || v === 'REGISTRY');
            document.getElementById('targetSchemaRegistryGroup').classList.toggle('hidden', v !== 'REGISTRY');
        });
    }

    // Transformation Type Toggle (INLINE vs FILE)
    document.querySelectorAll('input[name="transformationType"]').forEach(r => {
        r.addEventListener('change', () => {
            const isFile = r.value === 'FILE' && r.checked;
            document.getElementById('sqlContentGroup').classList.toggle('hidden', isFile);
            document.getElementById('sqlFileGroup').classList.toggle('hidden', !isFile);
        });
    });

    function showNotification(message, type) {
        const notification = document.getElementById('notification');
        const content = notification.querySelector('.notification-content');
        content.textContent = message;
        notification.className = `notification show ${type}`;
        setTimeout(() => notification.classList.remove('show'), 5000);
    }

    // --- Tab Navigation ---
    const tabBtns = document.querySelectorAll('.tab-btn');
    tabBtns.forEach(btn => {
        btn.addEventListener('click', () => {
            tabBtns.forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
            const tab = btn.dataset.tab;
            document.querySelectorAll('.tab-content').forEach(c => c.classList.add('hidden'));
            document.getElementById('tab-' + tab).classList.remove('hidden');
            document.getElementById('loadConfigBtn').style.display = tab === 'submit' ? '' : 'none';
            if (tab === 'dashboard') { loadRunningJobs(); loadJobAuditHistory(); }
        });
    });

    // --- Dashboard / Running Jobs ---
    async function loadRunningJobs() {
        const container = document.getElementById('jobsTableContainer');
        container.innerHTML = '<div class="empty-state">Loading...</div>';
        try {
            const response = await fetch('/api/jobs/list');
            const jobs = await response.json();
            if (!jobs || jobs.length === 0) {
                container.innerHTML = '<div class="empty-state">No jobs found. Submit a job to see it here.</div>';
                return;
            }
            const table = document.createElement('table');
            table.className = 'jobs-table';
            table.innerHTML = `
                <thead><tr>
                    <th>Job Name</th>
                    <th>Job ID</th>
                    <th>Status</th>
                </tr></thead>
                <tbody></tbody>`;
            const tbody = table.querySelector('tbody');
            jobs.forEach(job => {
                const tr = document.createElement('tr');
                tr.className = 'clickable-row';
                tr.onclick = () => showJobDetails(job.jobName);
                const statusClass = job.status || 'UNKNOWN';
                tr.innerHTML = `
                    <td>${job.jobName}</td>
                    <td style="font-family:monospace;font-size:0.8rem;">${job.jobId}</td>
                    <td><span class="job-status ${statusClass}">${statusClass}</span></td>`;
                tbody.appendChild(tr);
            });
            container.innerHTML = '';
            container.appendChild(table);
        } catch (e) {
            container.innerHTML = '<div class="empty-state" style="color:#ef4444;">Failed to load jobs: ' + e.message + '</div>';
        }
    }

    document.getElementById('refreshJobsBtn').addEventListener('click', loadRunningJobs);

    // Config Loader
    const configLoader = document.getElementById('configLoader');
    if (configLoader) {
        configLoader.addEventListener('change', (e) => {
            const file = e.target.files[0];
            if (!file) return;

            const reader = new FileReader();
            reader.onload = (e) => {
                try {
                    const config = JSON.parse(e.target.result);
                    populateForm(config);
                    showNotification('Configuration loaded successfully', 'success');
                } catch (err) {
                    showNotification('Failed to parse configuration file', 'error');
                } finally {
                    configLoader.value = '';
                }
            };
            reader.readAsText(file);
        });
    }

    function populateForm(config) {
        document.getElementById('jobName').value = config.jobName || '';
        document.getElementById('parallelism').value = config.parallelism || 1;
        document.getElementById('checkpointInterval').value = config.checkpointInterval || 60000;
        document.getElementById('checkpointDir').value = config.checkpointDir || '';

        // Populate Sources
        sourcesContainer.innerHTML = ''; // Clear existing
        if (config.sources && Array.isArray(config.sources)) {
            config.sources.forEach(src => addSource(src));
        } else if (config.source) {
            // Backward compatibility for single source config
            addSource(config.source);
        } else {
            addSource(); // Default empty
        }

        // Target
        if (config.target) {
            const targetTypeEl = document.getElementById('targetType');
            if (targetTypeEl) targetTypeEl.value = config.target.targetType || 'KAFKA';

            document.getElementById('targetBootstrapServers').value = config.target.targetBootstrapServers || '';
            document.getElementById('targetTopic').value = config.target.targetTopic || '';
            document.getElementById('targetAuthType').value = config.target.targetAuthType || 'NONE';
            document.getElementById('targetFormat').value = config.target.targetFormat || 'STRING';

            // Target schema
            const targetSchemaTypeEl = document.getElementById('targetSchemaType');
            if (targetSchemaTypeEl) targetSchemaTypeEl.value = config.target.targetSchemaType || '';
            const targetSchemaEl = document.getElementById('targetSchema');
            if (targetSchemaEl) targetSchemaEl.value = config.target.targetSchema || '';
            const targetSchemaRegistryUrlEl = document.getElementById('targetSchemaRegistryUrl');
            if (targetSchemaRegistryUrlEl) targetSchemaRegistryUrlEl.value = config.target.targetSchemaRegistryUrl || '';
            const targetSchemaSubjectEl = document.getElementById('targetSchemaSubject');
            if (targetSchemaSubjectEl) targetSchemaSubjectEl.value = config.target.targetSchemaSubject || '';

            document.getElementById('targetUsername').value = config.target.targetUsername || '';
            document.getElementById('targetPassword').value = config.target.targetPassword || '';
            document.getElementById('targetMechanism').value = config.target.targetMechanism || 'PLAIN';

            const targetTruststoreLocationEl = document.getElementById('targetTruststoreLocation');
            if (targetTruststoreLocationEl) targetTruststoreLocationEl.value = config.target.targetTruststoreLocation || '';
            const targetTruststorePasswordEl = document.getElementById('targetTruststorePassword');
            if (targetTruststorePasswordEl) targetTruststorePasswordEl.value = config.target.targetTruststorePassword || '';
            const targetJaasConfigEl = document.getElementById('targetJaasConfig');
            if (targetJaasConfigEl) targetJaasConfigEl.value = config.target.targetJaasConfig || '';

            // Dispatch change events to update visibility
            document.getElementById('targetAuthType').dispatchEvent(new Event('change'));
            if (targetSchemaTypeEl) targetSchemaTypeEl.dispatchEvent(new Event('change'));
        }

        // Transformation
        if (config.transformation) {
            const transformationType = config.transformation.transformationType || 'INLINE';
            const radioEl = document.querySelector(`input[name="transformationType"][value="${transformationType}"]`);
            if (radioEl) {
                radioEl.checked = true;
                radioEl.dispatchEvent(new Event('change'));
            }

            document.getElementById('sqlQuery').value = config.transformation.sqlQuery || '';
            document.getElementById('resultTableName').value = config.transformation.resultTableName || '';

            const sqlFilePathEl = document.getElementById('sqlFilePath');
            if (sqlFilePathEl) sqlFilePathEl.value = config.transformation.sqlFilePath || '';
        }
    }

    // --- Savepoint path restore field toggle ---
    const savepointPathInput = document.getElementById('savepointPath');
    const allowNonRestoredStateRow = document.getElementById('allowNonRestoredStateRow');
    if (savepointPathInput) {
        savepointPathInput.addEventListener('input', () => {
            allowNonRestoredStateRow.style.display =
                savepointPathInput.value.trim() ? 'flex' : 'none';
        });
    }

    // --- Job Details Modal Logic ---
    const jobDetailsModal = document.getElementById('jobDetailsModal');
    const closeModalBtn = document.getElementById('closeModalBtn');

    closeModalBtn.addEventListener('click', () => {
        jobDetailsModal.classList.add('hidden');
        currentModalJob = null;
    });

    let currentModalJob = null;

    document.getElementById('savepointBtn').addEventListener('click', async () => {
        if (!currentModalJob) return;
        const btn = document.getElementById('savepointBtn');
        const msg = document.getElementById('savepointStatusMsg');
        btn.disabled = true;
        msg.textContent = 'Taking savepoint… this may take a moment.';
        try {
            const resp = await fetch(`/api/jobs/${currentModalJob}/savepoint`, { method: 'POST' });
            if (resp.ok) {
                const record = await resp.json();
                msg.textContent = 'Savepoint completed.';
                loadSavepoints(currentModalJob);
                showNotification('Savepoint saved: ' + record.savepointPath, 'success');
            } else {
                const err = await resp.text();
                msg.textContent = 'Failed: ' + err;
                showNotification('Savepoint failed: ' + err, 'error');
            }
        } catch (e) {
            msg.textContent = 'Error: ' + e.message;
        } finally {
            btn.disabled = false;
        }
    });

    document.getElementById('cancelJobBtn').addEventListener('click', async () => {
        if (!currentModalJob) return;
        if (!confirm(`Cancel job "${currentModalJob}"? This cannot be undone.`)) return;
        try {
            const resp = await fetch(`/api/jobs/${currentModalJob}`, { method: 'DELETE' });
            if (resp.ok) {
                showNotification(`Job "${currentModalJob}" cancelled.`, 'success');
                jobDetailsModal.classList.add('hidden');
                currentModalJob = null;
                loadRunningJobs();
            } else {
                showNotification('Cancel failed: ' + await resp.text(), 'error');
            }
        } catch (e) {
            showNotification('Cancel error: ' + e.message, 'error');
        }
    });

    async function loadSavepoints(jobName) {
        const ul = document.getElementById('savepointsList');
        try {
            const resp = await fetch(`/api/jobs/${jobName}/savepoints`);
            if (!resp.ok) { ul.innerHTML = '<li style="color:#aaa;font-family:sans-serif;font-size:0.85rem;">Could not load savepoints.</li>'; return; }
            const records = await resp.json();
            ul.innerHTML = '';
            if (!records || records.length === 0) {
                ul.innerHTML = '<li style="font-family:sans-serif;color:#aaa;font-size:0.85rem;">No savepoints yet.</li>';
                return;
            }
            // Newest first
            records.slice().reverse().forEach(r => {
                const li = document.createElement('li');
                const ts = new Date(r.createdAt).toLocaleString();
                const tag = r.cancelledJob ? ' <span style="color:#ef4444;">[cancel]</span>' : '';
                li.innerHTML = `<span title="${r.savepointPath}">${r.savepointPath}${tag}<br><small style="color:#888;">${ts}</small></span>
                    <button class="btn-copy-path" onclick="navigator.clipboard.writeText('${r.savepointPath}').then(()=>showNotification('Path copied','success'))">Copy</button>`;
                ul.appendChild(li);
            });
        } catch (e) {
            ul.innerHTML = '<li style="color:#ef4444;font-family:sans-serif;font-size:0.85rem;">Error loading savepoints.</li>';
        }
    }

    async function showJobDetails(jobName) {
        currentModalJob = jobName;
        document.getElementById('detailsTitle').textContent = `Job Details: ${jobName}`;
        document.getElementById('configSummary').textContent = 'Loading...';
        document.getElementById('reconciliationData').innerHTML = 'Loading...';
        document.getElementById('auditEventsList').innerHTML = '<li>Loading...</li>';
        document.getElementById('savepointStatusMsg').textContent = '';
        jobDetailsModal.classList.remove('hidden');
        loadSavepoints(jobName);

        // Fetch Config
        try {
            const configResp = await fetch(`/api/jobs/${jobName}`);
            if (configResp.ok) {
                const configJson = await configResp.json();
                document.getElementById('configSummary').textContent = JSON.stringify(configJson, null, 2);
            } else {
                document.getElementById('configSummary').textContent = 'Config not available.';
            }
        } catch (e) {
            document.getElementById('configSummary').textContent = 'Error fetching config.';
        }

        // Fetch Reconciliation
        try {
            const reconResp = await fetch(`/api/jobs/${jobName}/reconciliation`);
            if (reconResp.ok && reconResp.status !== 204) {
                const reconData = await reconResp.json();
                const status = reconData.reconciled ? '<span style="color:#22c55e;font-weight:bold;">Success</span>' : '<span style="color:#ef4444;font-weight:bold;">Failed</span>';
                document.getElementById('reconciliationData').innerHTML = `
                    <b>Status:</b> ${status}<br>
                    <b>Window:</b> ${reconData.windowStart} &rarr; ${reconData.windowEnd} (${reconData.windowLabel})<br>
                    <b>Source Read:</b> ${reconData.sourceReadCount}<br>
                    <b>Rejected:</b> ${reconData.schemaRejectedCount}<br>
                    <b>Transformed:</b> ${reconData.transformedCount}<br>
                    <b>Target Written:</b> ${reconData.targetWrittenCount}<br>
                    <b>Discrepancies:</b> ${reconData.discrepancies && reconData.discrepancies.length > 0 ? reconData.discrepancies.join(', ') : 'None'}
                `;
            } else {
                document.getElementById('reconciliationData').innerHTML = '<i>No reconciliation report found yet.</i>';
            }
        } catch (e) {
            document.getElementById('reconciliationData').innerHTML = 'Error fetching reconciliation.';
        }

        // Fetch Audit Events
        try {
            const auditResp = await fetch(`/api/jobs/${jobName}/audit`);
            if (auditResp.ok) {
                const auditEvents = await auditResp.json();
                const ul = document.getElementById('auditEventsList');
                ul.innerHTML = '';
                if (auditEvents.length === 0) {
                    ul.innerHTML = '<li><i>No audit events found.</i></li>';
                } else {
                    // Sorting backwards so newest is on top
                    auditEvents.slice().reverse().forEach(evt => {
                        const li = document.createElement('li');
                        // Formatting time
                        const timeStr = new Date(evt.timestamp).toLocaleTimeString();
                        let countSegment = evt.count > 0 ? ` <span style="color:#aaa;">(Count: ${evt.count})</span>` : '';
                        li.innerHTML = `<span><strong>${evt.eventType}</strong> <small style="color:#888;">[${evt.stage}]</small>${countSegment}</span> <span style="font-family:monospace;">${timeStr}</span>`;
                        ul.appendChild(li);
                    });
                }
            } else {
                document.getElementById('auditEventsList').innerHTML = '<li>Error fetching audit logs.</li>';
            }
        } catch (e) {
            document.getElementById('auditEventsList').innerHTML = '<li>Error fetching audit logs.</li>';
        }
    }

    // ── Feature 008: Job Audit History ───────────────────────────────────────

    async function loadJobAuditHistory() {
        const tbody = document.getElementById('auditHistoryRows');
        if (!tbody) return;
        tbody.innerHTML = '<tr><td colspan="7" style="padding:16px;text-align:center;color:#aaa;">Loading...</td></tr>';
        try {
            const res = await fetch('/api/dashboard/jobs');
            if (!res.ok) { tbody.innerHTML = '<tr><td colspan="7" style="color:#ef4444;padding:16px;">Failed to load history</td></tr>'; return; }
            const jobs = await res.json();
            if (!jobs.length) {
                tbody.innerHTML = '<tr><td colspan="7" style="padding:16px;text-align:center;color:#aaa;">No job history yet</td></tr>';
                return;
            }
            tbody.innerHTML = '';
            const colors = { RUNNING:'#22c55e', FINISHED:'#3b82f6', FAILED:'#ef4444', CANCELLED:'#6b7280', SUBMITTING:'#f59e0b' };
            jobs.forEach(job => {
                const color = colors[job.status] || '#9ca3af';
                const shortId = job.flinkJobId ? job.flinkJobId.substring(0, 8) + '…' : '—';
                const submitted = job.submittedAt ? new Date(job.submittedAt).toLocaleString() : '—';
                const stopBtn = job.status === 'RUNNING'
                    ? `<button onclick="stopAuditJob(${job.id})" style="margin-right:4px;cursor:pointer;">Stop</button>` : '';
                const delBtn = job.status !== 'RUNNING' && job.status !== 'SUBMITTING'
                    ? `<button onclick="deleteAuditRecord(${job.id})" style="cursor:pointer;">Delete</button>` : '';
                const tr = document.createElement('tr');
                tr.style.borderBottom = '1px solid rgba(255,255,255,0.06)';
                tr.innerHTML = `
                    <td style="padding:8px;">${job.id}</td>
                    <td style="padding:8px;font-weight:500;">${job.jobName}</td>
                    <td style="padding:8px;font-family:monospace;font-size:0.78rem;">${shortId}</td>
                    <td style="padding:8px;"><span style="background:${color};color:#fff;padding:2px 8px;border-radius:12px;font-size:0.78rem;">${job.status}</span></td>
                    <td style="padding:8px;">${job.parallelism || '—'}</td>
                    <td style="padding:8px;font-size:0.8rem;">${submitted}</td>
                    <td style="padding:8px;">${stopBtn}${delBtn}</td>`;
                tbody.appendChild(tr);
            });
        } catch (e) {
            tbody.innerHTML = '<tr><td colspan="7" style="color:#ef4444;padding:16px;">Error: ' + e.message + '</td></tr>';
        }
    }

    async function stopAuditJob(id) {
        if (!confirm('Stop this job?')) return;
        const res = await fetch('/api/dashboard/jobs/' + id + '/stop', { method: 'POST' });
        showNotification(await res.text(), res.ok ? 'success' : 'error');
        loadJobAuditHistory();
        loadRunningJobs();
    }

    async function deleteAuditRecord(id) {
        if (!confirm('Delete this audit record?')) return;
        const res = await fetch('/api/dashboard/jobs/' + id, { method: 'DELETE' });
        showNotification(await res.text(), res.ok ? 'success' : 'error');
        loadJobAuditHistory();
    }

    // Auto-refresh audit history every 30s when dashboard tab is visible
    setInterval(() => {
        const dash = document.getElementById('tab-dashboard');
        if (dash && !dash.classList.contains('hidden')) loadJobAuditHistory();
    }, 30000);

});
