const socket = io();

let sessionId = null;
let isConnected = false;
let isProgressLineInputFocused = false; // New flag to track focus

const PASTE_LINE_LIMIT_COMBO = 10000;
const PASTE_LINE_LIMIT_PROXY = 5000;

// --- stop locks ---
let isStopping = false;
let forceStopped = false;
function setControlsDisabled(disabled) {
    ['start-button','pause-button','continue-button','set-progress-button']
      .forEach(id => { const el = document.getElementById(id); if (el) el.disabled = disabled; });
}
function resetUIToFreshSession() {
    const zeros = {'total-lines':'0','checked':'0','invalid':'0','hits':'0','custom':'0','total-mega-fan':'0','total-fan-member':'0','total-ultimate-mega':'0','errors':'0','retries':'0','cpm':'0','elapsed-time':'0:00:00'};
    Object.entries(zeros).forEach(([id,val]) => { const el = document.getElementById(id); if (el) el.textContent = val; });
    const statusEl = document.getElementById('status'); if (statusEl) { statusEl.textContent = '❌ STOPPED'; statusEl.style.color = '#ff6b6b'; }
    const combo = document.getElementById('combo-input'); if (combo) { combo.value=''; combo.style.height='auto'; }
    const proxy = document.getElementById('proxy-input'); if (proxy) { proxy.value=''; proxy.style.height='auto'; }
    const progress = document.getElementById('progress-line-input'); if (progress) progress.value = 0;
}


function showStatusMessage(message, type = 'info') {
    const statusContainer = document.getElementById('status-messages');
    const messageDiv = document.createElement('div');
    messageDiv.className = `status-message ${type}`;
    messageDiv.textContent = message;

    statusContainer.appendChild(messageDiv);

    setTimeout(() => {
        if (messageDiv.parentNode) {
            messageDiv.style.opacity = '0';
            messageDiv.style.transform = 'translateX(100%)';
            setTimeout(() => {
                if (messageDiv.parentNode) {
                    statusContainer.removeChild(messageDiv);
                }
            }, 300);
        }
    }, 5000);
}

socket.on('connect', () => {
    isConnected = true;
    showStatusMessage('Connected to server successfully!', 'success');
    const storedSessionId = localStorage.getItem('sessionId');
    if (storedSessionId) {
        socket.emit('reconnect_session', { session_id: storedSessionId });
    } else {
        socket.emit('request_session');
    }
});

socket.on('disconnect', () => {
    isConnected = false;
    showStatusMessage('Disconnected from server!', 'error');
});

socket.on('session_created', (data) => {
    sessionId = data.session_id;
    localStorage.setItem('sessionId', sessionId);
    console.log(`Session created: ${sessionId}`);
    showStatusMessage('Session created successfully!', 'success');
    history.pushState({ sessionId: sessionId }, '', `?session=${sessionId}`);
    document.getElementById('combo-input').value = '';
    document.getElementById('proxy-input').value = '';
    document.getElementById('progress-line-input').value = 0;
    document.getElementById('threads-input').value = 10;
    document.getElementById('proxy-type-select').value = 'http';
    updateStatsDisplay({
        status: '❌ STOPPED',
        total_lines: 0,
        checked: 0,
        invalid: 0,
        hits: 0,
        custom: 0,
        total_mega_fan: 0,
        total_fan_member: 0,
        total_ultimate_mega: 0,
        errors: 0,
        retries: 0,
        cpm: 0,
        elapsed_time: '0:00:00'
    });
});

socket.on('session_reconnected', (data) => {
    sessionId = data.session_id;
    console.log(`Session reconnected: ${sessionId}`);
    showStatusMessage('Session reconnected successfully!', 'success');
    history.pushState({ sessionId: sessionId }, '', `?session=${sessionId}`);
    if (data.previous_state) {
        updateStatsDisplay(data.previous_state.stats);
        document.getElementById('combo-input').value = data.previous_state.combo_file_uploaded ? `Combo file uploaded (${data.previous_state.stats.total_lines} lines). Ready to check. ✅` : '';
        document.getElementById('proxy-input').value = data.previous_state.proxy_file_uploaded ? `Proxy file uploaded. Ready to check. ✅` : '';
        document.getElementById('threads-input').value = data.previous_state.threads || 10;
        document.getElementById('proxy-type-select').value = data.previous_state.proxy_type || 'http';
        // Only update progress-line-input if it's not currently focused
        if (!isProgressLineInputFocused) {
            document.getElementById('progress-line-input').value = data.previous_state.stats.checked || 0;
        }
        
        const textareas = document.querySelectorAll('.upload-textarea');
        textareas.forEach(textarea => {
            textarea.style.height = 'auto';
            textarea.style.height = Math.min(textarea.scrollHeight, 300) + 'px';
        });
    }
});

socket.on('stats_update', (data) => {
    if ((isStopping || forceStopped) && !(data.status && data.status.includes('STOPPED'))) { return; }
    updateStatsDisplay(data);
    const progressLineInput = document.getElementById('progress-line-input');
    // Only update progress-line-input if it's not currently focused AND its value matches the current checked count
    // This prevents overwriting user input if they are actively changing it.
    if (!isProgressLineInputFocused && parseInt(progressLineInput.value) === data.checked) {
        progressLineInput.value = data.checked;
    }
});

socket.on('error', (data) => {
    showStatusMessage(data.message, 'error');
});

socket.on('combo_uploaded', (data) => {
    showStatusMessage(data.message, 'success');
    document.getElementById('total-lines').textContent = data.count;
    document.getElementById('combo-input').value = `Combo file uploaded (${data.count} lines). Ready to check. ✅`;
    const textarea = document.getElementById('combo-input');
    textarea.style.height = 'auto';
    textarea.style.height = Math.min(textarea.scrollHeight, 300) + 'px';
});

socket.on('proxy_uploaded', (data) => {
    showStatusMessage(data.message, 'success');
    document.getElementById('proxy-input').value = `Proxy file uploaded (${data.count} lines). Ready to check. ✅`;
    document.getElementById('proxy-type-select').value = data.proxy_type;
    const textarea = document.getElementById('proxy-input');
    textarea.style.height = 'auto';
    textarea.style.height = Math.min(textarea.scrollHeight, 300) + 'px';
});

socket.on('checker_started', (data) => {
    showStatusMessage(data.message, 'success');
});

socket.on('checker_stopped', (data) => {
    forceStopped = true; isStopping = false;
    resetUIToFreshSession(); setControlsDisabled(false);
    showStatusMessage(data.message, 'info');
});

socket.on('checker_paused', (data) => {
    showStatusMessage(data.message, 'info');
});

socket.on('checker_continued', (data) => {
    showStatusMessage(data.message, 'success');
});

socket.on('checker_completed', (data) => {
    showStatusMessage(data.message, 'success');
});
socket.on('inputs_cleared', () => {
    forceStopped = true; isStopping = false;
    resetUIToFreshSession(); setControlsDisabled(false);
});


socket.on('hits_available', (data) => {
    showStatusMessage('Hits file is ready for download!', 'success');
    downloadFile(data.content, data.filename);
});

socket.on('hits_download', (data) => {
    downloadFile(data.content, data.filename);
    showStatusMessage('Hits downloaded successfully!', 'success');
});

socket.on('progress_line_updated', (data) => {
    showStatusMessage(data.message, 'success');
    // Update the displayed checked count and the input field
    document.getElementById('checked').textContent = data.new_checked_count;
    document.getElementById('progress-line-input').value = data.new_checked_count;
});

function updateStatsDisplay(data) {
    document.getElementById('status').textContent = data.status;
    document.getElementById('total-lines').textContent = data.total_lines;
    document.getElementById('checked').textContent = data.checked;
    document.getElementById('invalid').textContent = data.invalid;
    document.getElementById('hits').textContent = data.hits;
    document.getElementById('custom').textContent = data.custom;
    document.getElementById('total-mega-fan').textContent = data.total_mega_fan;
    document.getElementById('total-fan-member').textContent = data.total_fan_member;
    document.getElementById('total-ultimate-mega').textContent = data.total_ultimate_mega;
    document.getElementById('errors').textContent = data.errors;
    document.getElementById('retries').textContent = data.retries;
    document.getElementById('cpm').textContent = data.cpm;
    document.getElementById('elapsed-time').textContent = data.elapsed_time;

    const statusElement = document.getElementById('status');
    if (data.status.includes('RUNNING')) {
        statusElement.style.color = '#4ecdc4';
    } else if (data.status.includes('PAUSED')) {
        statusElement.style.color = '#feca57';
    } else if (data.status.includes('COMPLETE')) {
        statusElement.style.color = '#4ecdc4';
    } else {
        statusElement.style.color = '#ff6b6b';
    }
}

function downloadFile(content, filename) {
    const blob = new Blob([content], { type: 'text/plain' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
}

function validateSession() {
    if (!sessionId) {
        showStatusMessage('No active session. Please refresh the page or wait for connection.', 'error');
        return false;
    }
    if (!isConnected) {
        showStatusMessage('Not connected to server. Please check your connection.', 'error');
        return false;
    }
    return true;
}

function uploadFileViaHttp(file, fileType) {
    if (!validateSession()) return;

    const formData = new FormData();
    formData.append('file', file);
    formData.append('session_id', sessionId);
    formData.append('file_type', fileType);
    if (fileType === 'proxy') {
        formData.append('proxy_type', document.getElementById('proxy-type-select').value);
    }

    const xhr = new XMLHttpRequest();
    xhr.open('POST', '/upload_file', true);

    xhr.onload = function() {
        if (xhr.status === 200) {
            const response = JSON.parse(xhr.responseText);
            console.log(`${fileType} upload success:`, response.message);
            // Emit the success event to update UI
            // These events are already handled by socket.io.on('combo_uploaded') etc.
            // No need to manually emit here. The server will emit.
        } else {
            const errorResponse = JSON.parse(xhr.responseText);
            showStatusMessage(`Error uploading ${fileType}: ${errorResponse.message}`, 'error');
            console.error(`Error uploading ${fileType}:`, errorResponse);
            if (fileType === 'combo') document.getElementById('combo-input').value = '';
            if (fileType === 'proxy') document.getElementById('proxy-input').value = '';
        }
    };

    xhr.onerror = function() {
        showStatusMessage(`Network error during ${fileType} upload. Please check server connection.`, 'error');
        console.error(`Network error during ${fileType} upload.`);
        if (fileType === 'combo') document.getElementById('combo-input').value = '';
        if (fileType === 'proxy') document.getElementById('proxy-input').value = '';
    };

    xhr.send(formData);
    showStatusMessage(`Uploading ${fileType} file...`, 'info');
}

function uploadContentViaWebSocket(content, fileType) {
    if (!validateSession()) return;

    if (fileType === 'combo') {
        socket.emit('upload_combo', {
            session_id: sessionId,
            content: content
        });
    } else if (fileType === 'proxy') {
        socket.emit('upload_proxy', {
            session_id: sessionId,
            content: content,
            proxy_type: document.getElementById('proxy-type-select').value
        });
    }
    showStatusMessage(`Processing pasted ${fileType} content...`, 'info');
}


document.addEventListener('DOMContentLoaded', function() {

    function triggerFileInput(fileType) {
        const input = document.createElement('input');
        input.type = 'file';
        input.accept = '.txt';
        input.onchange = (e) => {
            const file = e.target.files[0];
            if (file) {
                uploadFileViaHttp(file, fileType);
            }
        };
        input.click();
    }

    document.getElementById('upload-combo-button').addEventListener('click', () => {
        if (!validateSession()) return;

        const comboContent = document.getElementById('combo-input').value.trim();

        if (comboContent) {
            const lineCount = comboContent.split('\n').length;
            if (lineCount > PASTE_LINE_LIMIT_COMBO) {
                showStatusMessage(`Pasted combo content is too large (${lineCount} lines). Please use the file upload dialog for files larger than ${PASTE_LINE_LIMIT_COMBO} lines.`, 'error');
                document.getElementById('combo-input').value = '';
                return;
            }
            uploadContentViaWebSocket(comboContent, 'combo');
        } else {
            triggerFileInput('combo');
        }
    });

    document.getElementById('upload-proxy-button').addEventListener('click', () => {
        if (!validateSession()) return;

        const proxyContent = document.getElementById('proxy-input').value.trim();
        
        if (proxyContent) {
            const lineCount = proxyContent.split('\n').length;
            if (lineCount > PASTE_LINE_LIMIT_PROXY) {
                showStatusMessage(`Pasted proxy content is too large (${lineCount} lines). Please use the file upload dialog for files larger than ${PASTE_LINE_LIMIT_PROXY} lines.`, 'error');
                document.getElementById('proxy-input').value = '';
                return;
            }
            uploadContentViaWebSocket(proxyContent, 'proxy');
        } else {
            triggerFileInput('proxy');
        }
    });

    document.getElementById('start-button').addEventListener('click', () => { forceStopped = false;
        if (!validateSession()) return;

        const threads = parseInt(document.getElementById('threads-input').value);
        const initialProgressLine = parseInt(document.getElementById('progress-line-input').value);
        const proxyType = document.getElementById('proxy-type-select').value; // Get current proxy type

        if (isNaN(threads) || threads < 1 || threads > 400) {
            showStatusMessage('Please enter a valid thread count (1-400)!', 'error');
            return;
        }
        if (isNaN(initialProgressLine) || initialProgressLine < 0) {
            showStatusMessage('Please enter a valid non-negative number for the progress line!', 'error');
            return;
        }

        socket.emit('start_checker', {
            session_id: sessionId,
            threads: threads,
            initial_progress_line: initialProgressLine,
            proxy_type: proxyType // Pass proxy type to backend
        });
    });

    document.getElementById('stop-button').addEventListener('click', () => {
        if (!validateSession()) return;
        if (isStopping) return;
        isStopping = true; forceStopped = true;
        setControlsDisabled(true);
        showStatusMessage('Stopping... please wait a moment.', 'info');
        socket.emit('stop_checker', { session_id: sessionId });
    });

    document.getElementById('pause-button').addEventListener('click', () => {
        if (!validateSession()) return;

        socket.emit('pause_checker', {
            session_id: sessionId
        });
    });

    document.getElementById('continue-button').addEventListener('click', () => { forceStopped = false;
        if (!validateSession()) return;

        const currentThreads = parseInt(document.getElementById('threads-input').value);
        const currentProxyType = document.getElementById('proxy-type-select').value;

        socket.emit('continue_checker', {
            session_id: sessionId,
            threads: currentThreads,
            proxy_type: currentProxyType
        });
    });

    document.getElementById('download-hits-button').addEventListener('click', () => {
        if (!validateSession()) return;

        socket.emit('download_hits', {
            session_id: sessionId
        });
    });

    const progressLineInput = document.getElementById('progress-line-input');
    progressLineInput.addEventListener('focus', () => {
        isProgressLineInputFocused = true;
    });
    progressLineInput.addEventListener('blur', () => {
        isProgressLineInputFocused = false;
    });

    document.getElementById('set-progress-button').addEventListener('click', () => {
        if (!validateSession()) return;

        const newProgressLine = parseInt(document.getElementById('progress-line-input').value);

        if (isNaN(newProgressLine) || newProgressLine < 0) {
            showStatusMessage('Please enter a valid non-negative number for the progress line!', 'error');
            return;
        }

        socket.emit('set_progress_line', {
            session_id: sessionId,
            progress_line: newProgressLine
        });
    });

    const textareas = document.querySelectorAll('.upload-textarea');
    textareas.forEach(textarea => {
        textarea.addEventListener('input', function() {
            this.style.height = 'auto';
            this.style.height = Math.min(this.scrollHeight, 300) + 'px';
        });

        textarea.addEventListener('paste', function(event) {
            const pastedText = (event.clipboardData || window.clipboardData).getData('text');
            const lineCount = pastedText.split('\n').length;
            const isComboTextarea = this.id === 'combo-input';
            const limit = isComboTextarea ? PASTE_LINE_LIMIT_COMBO : PASTE_LINE_LIMIT_PROXY;

            if (lineCount > limit) {
                event.preventDefault();
                showStatusMessage(`Pasted ${isComboTextarea ? 'combo' : 'proxy'} content is too large (${lineCount} lines). Please use the file upload dialog for files larger than ${limit} lines.`, 'error');
                this.value = '';
            }
        });
    });

    document.addEventListener('keydown', function(e) {
        if (e.ctrlKey || e.metaKey) {
            switch(e.key) {
                case 's':
                    e.preventDefault();
                    document.getElementById('start-button').click();
                    break;
                case 'q':
                    e.preventDefault();
                    document.getElementById('stop-button').click();
                    break;
                case 'p':
                    e.preventDefault();
                    document.getElementById('pause-button').click();
                    break;
                case 'r':
                    e.preventDefault();
                    document.getElementById('continue-button').click();
                    break;
                case 'd':
                    e.preventDefault();
                    document.getElementById('download-hits-button').click();
                    break;
                case 'g':
                    e.preventDefault();
                    document.getElementById('set-progress-button').click();
                    break;
            }
        }
    });

    document.getElementById('start-button').title = 'Start Checker (Ctrl+S)';
    document.getElementById('stop-button').title = 'Stop Checker (Ctrl+Q)';
    document.getElementById('pause-button').title = 'Pause Checker (Ctrl+P)';
    document.getElementById('continue-button').title = 'Continue Checker (Ctrl+R)';
    document.getElementById('download-hits-button').title = 'Download Hits (Ctrl+D)';
    document.getElementById('set-progress-button').title = 'Set Progress Line (Ctrl+G)';

    const urlParams = new URLSearchParams(window.location.search);
    const urlSessionId = urlParams.get('session');
    if (urlSessionId) {
        localStorage.setItem('sessionId', urlSessionId);
    }
});

document.addEventListener('click', function(e) {
    if (e.target.classList.contains('btn-3d')) {
        const ripple = document.createElement('span');
        const rect = e.target.getBoundingClientRect();
        const size = Math.max(rect.width, rect.height);
        const x = e.clientX - rect.left - size / 2;
        const y = e.clientY - rect.top - size / 2;

        ripple.style.width = ripple.style.height = size + 'px';
        ripple.style.left = x + 'px';
        ripple.style.top = y + 'px';
        ripple.classList.add('ripple');

        e.target.appendChild(ripple);

        setTimeout(() => {
            if (ripple.parentNode) {
                ripple.parentNode.removeChild(ripple);
            }
        }, 600);
    }
});

const style = document.createElement('style');
style.textContent = `
    .btn-3d {
        position: relative;
        overflow: hidden;
    }

    .ripple {
        position: absolute;
        border-radius: 50%;
        background: rgba(255, 255, 255, 0.6);
        transform: scale(0);
        animation: ripple-animation 0.6s linear;
        pointer-events: none;
    }

    @keyframes ripple-animation {
        to {
            transform: scale(4);
            opacity: 0;
        }
    }
`;
document.head.appendChild(style);

