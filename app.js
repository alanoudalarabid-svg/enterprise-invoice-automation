
document.addEventListener('DOMContentLoaded', function () {
    const uploadZone = document.getElementById('uploadZone');
    const fileInput = document.getElementById('file-input');
    const loadingIndicator = document.getElementById('loadingIndicator');
    const progressBar = document.getElementById('progressBar');
    const totalFilesElement = document.getElementById('totalFiles');
    const processedFilesElement = document.getElementById('processedFiles');
    const toBeProcessedElement = document.getElementById('toBeProcessed');
    const processBtn = document.getElementById('processBtn');
    const cancelBtn = document.getElementById('cancelBtn');
    const batchControls = document.getElementById('batchControls');
    const failedFilesElement = document.getElementById('failedFiles');
    
    let totalFiles = 0;
    let failedCount = 0;
    let processedCount = 0;
    let currentBatch = [];
    let isProcessing = false;
    let abortController = new AbortController();
    let currentProcessingIndex = 0;

    function initApp() {
        resetUploadUI();
        isProcessing = false;
        currentBatch = [];
        processedCount = 0;
        totalFiles = 0;
        currentProcessingIndex = 0;
        abortController = new AbortController();
        fileInput.disabled = false;
        document.querySelector('label[for="file-input"]').classList.remove('disabled-upload-label');
        cancelBtn.classList.remove('start-new-mode');
    }

    function resetUploadUI() {
        totalFilesElement.textContent = '0';
        processedFilesElement.textContent = '0';
        toBeProcessedElement.textContent = '0';
        progressBar.style.width = '0%';
        progressBar.classList.remove('completed');
        fileInput.value = '';
        currentBatch = [];
        batchControls.style.display = 'none';
        uploadZone.classList.remove('highlight', 'cancelling');
        processBtn.disabled = false;
        cancelBtn.innerHTML = '<i class="fas fa-upload"></i> Start New Upload';
        cancelBtn.disabled = false;
        cancelBtn.classList.add('start-new-mode');
        fileInput.disabled = true;
    }

    function logErrorToServer(filename, errorDetails, stage) {
        fetch('/log_error', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                filename: filename,
                error: errorDetails,
                stage: stage,
                timestamp: new Date().toISOString()
            })
        }).catch(err => console.error('Error logging failed:', err));
    }

    function updateProgress(processed) {
        const attempted = processed + failedCount;
        const progressPercent = Math.round((attempted / currentBatch.length) * 100);
        progressBar.style.width = `${progressPercent}%`;
        processedFilesElement.textContent = processed;
        toBeProcessedElement.textContent = Math.max(0, currentBatch.length - attempted);
    
        if (progressPercent === 100) {
            progressBar.classList.add('completed');
        }
    }

    function resetProcessingState() {
        processedCount = 0;
        failedCount = 0;
        currentProcessingIndex = 0;
        updateProgress(0);
        progressBar.classList.remove('completed');
        processedFilesElement.textContent = '0';
        failedFilesElement.textContent = '0';
        toBeProcessedElement.textContent = totalFiles;
    }

    function handleFiles(files) {
        resetProcessingState();
        currentBatch = Array.from(files);

        if (currentBatch.length === 0) return;

        totalFiles = currentBatch.length;
        totalFilesElement.textContent = totalFiles;
        toBeProcessedElement.textContent = totalFiles;

        batchControls.style.display = 'block';
        fileInput.disabled = true;
        cancelBtn.classList.remove('start-new-mode');
        cancelBtn.innerHTML = '<i class="fas fa-times"></i> Cancel';
    }

    async function processBatch() {
        if (isProcessing) return;
    
        isProcessing = true;
        processedCount = 0;
        failedCount = 0;
        currentProcessingIndex = 0;
        updateProgress(0);
        failedFilesElement.textContent = '0';
        abortController = new AbortController();
    
        loadingIndicator.style.display = 'block';
        processBtn.disabled = true;
        cancelBtn.disabled = false;
    
        try {
            for (let i = 0; i < currentBatch.length; i++) {
                if (abortController.signal.aborted) break;

                currentProcessingIndex = i;
                const file = currentBatch[i];

                try {
                    const uploadResponse = await Promise.race([
                        fetch('/upload', {
                            method: 'POST',
                            body: (() => {
                                const formData = new FormData();
                                formData.append('file', file);
                                return formData;
                            })(),
                            signal: abortController.signal
                        }),
                        new Promise((_, reject) =>
                            setTimeout(() => reject(new Error('upload timeout')), 30000)
                        ) 
                    ]);

                    if (!uploadResponse.ok) {
                        const errorText = await uploadResponse.text();
                        throw new Error(errorText || 'Upload failed');
                    }

                    const uploadResult = await uploadResponse.json();
                    if (!uploadResult.success) throw new Error(uploadResult.message || 'Invalid file');

                    const processResponse = await Promise.race([
                        fetch('/process_single', {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json' },
                            body: JSON.stringify({
                                filename: uploadResult.filename,
                                filepath: uploadResult.filepath,
                                attempt: currentProcessingIndex + 1
                            }),
                            signal: abortController.signal
                        }),
                        new Promise((_, reject) =>
                            setTimeout(() => reject(new Error('processing timeout')), 45000)
                        )
                    ]);

                    const processResult = await processResponse.json();

                    if (processResult.success) {
                        processedCount++;
                    } else {
                        let verified = false;
                        for (let attempt = 1; attempt <= 3; attempt++) {
                            try {
                                const verifyResponse = await fetch('/verify_processing', {
                                    method: 'POST',
                                    headers: { 'Content-Type': 'application/json' },
                                    body: JSON.stringify({
                                        filename: uploadResult.filename,
                                        strict_check: true,
                                        attempt: attempt
                                    }),
                                    signal: abortController.signal
                                });
                                const verifyResult = await verifyResponse.json();
                                if (verifyResult.exists_in_db) {
                                    processedCount++;
                                    verified = true;
                                    break;
                                }
                            } catch (error) {
                                if (attempt === 3) logErrorToServer(file.name, error.message, 'verification');
                                if (attempt < 3) await new Promise(resolve => setTimeout(resolve, 1000 * attempt));
                            }
                        }

                        if (!verified) {
                            failedCount++;
                            logErrorToServer(file.name, processResult.message || 'Processing failed', 'processing');
                        }
                    }

                } catch (error) {
                    if (error.name === 'AbortError') break;

                    failedCount++;
                    logErrorToServer(file.name, error.message, error.message.includes('upload') ? 'upload' : 'processing');
                }

                failedFilesElement.textContent = failedCount;
                updateProgress(processedCount);
            }

        } finally {
            isProcessing = false;
            loadingIndicator.style.display = 'none';
            fileInput.disabled = true;
            document.querySelector('label[for="file-input"]').classList.add('disabled-upload-label');
            cancelBtn.innerHTML = '<i class="fas fa-upload"></i> Start New Upload';
            cancelBtn.classList.add('start-new-mode');
        }
    }

    // Drag and drop handlers
    ['dragenter', 'dragover', 'dragleave', 'drop'].forEach(eventName => {
        uploadZone.addEventListener(eventName, preventDefaults, false);
        document.body.addEventListener(eventName, preventDefaults, false);
    });

    function preventDefaults(e) {
        e.preventDefault();
        e.stopPropagation();
    }

    ['dragenter', 'dragover'].forEach(eventName => {
        uploadZone.addEventListener(eventName, () => uploadZone.classList.add('highlight'), false);
    });


    ['dragleave', 'drop'].forEach(eventName => {
        uploadZone.addEventListener(eventName, () => uploadZone.classList.remove('highlight'), false);
    });

    uploadZone.addEventListener('drop', function(e) {
        handleFiles(e.dataTransfer.files);
    });

    fileInput.addEventListener('change', function() {
        handleFiles(this.files);
    });

    processBtn.addEventListener('click', function() {
        if (currentBatch.length === 0) return;
        resetProcessingState();
        processBatch();
    });

    cancelBtn.addEventListener('click', async function () {
        if (isProcessing) {
            uploadZone.classList.add('cancelling');
            cancelBtn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Cancelling...';
            cancelBtn.disabled = true;

            try {
                abortController.abort();
                const remainingFiles = currentBatch.slice(currentProcessingIndex).map(f => f.name);
                if (remainingFiles.length > 0) {
                    await fetch('/cancel_batch', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ filenames: remainingFiles })
                    });
                }
            } finally {
                uploadZone.classList.remove('cancelling');
                isProcessing = false;
                fileInput.disabled = false;
                cancelBtn.innerHTML = '<i class="fas fa-upload"></i> Start New Upload';
                cancelBtn.classList.add('start-new-mode');
            }
        } else {
            initApp();
        }
    });

    initApp();
});