import os
import threading
import time
import shutil
from datetime import datetime, timedelta
import json
import uuid
import io
import logging

from flask import Flask, render_template, request, jsonify, send_file
from flask_socketio import SocketIO, emit
from colorama import Fore, init


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
init()


# ==== Added helpers: crash-safe snapshot + dedupe ====
import os, json
from datetime import datetime, timedelta

RUNTIME_STATE_FILE = "runtime_state.json"

def _runtime_state_path(session_id):
    try:
        session_dir = ensure_session_directory(session_id)
    except Exception:
        session_dir = os.path.join(os.getcwd(), f"session_{session_id}")
        os.makedirs(session_dir, exist_ok=True)
    return os.path.join(session_dir, RUNTIME_STATE_FILE)

def save_runtime_state(session_id):
    try:
        c = chk.counters.get(session_id, {})
        if not c:
            return
        payload = {
            'checked': int(c.get('checked', 0)),
            'invalid': int(c.get('invalid', 0)),
            'hits': int(c.get('hits', 0)),
            'custom': int(c.get('custom', 0)),
            'total_mega_fan': int(c.get('total_mega_fan', 0)),
            'total_fan_member': int(c.get('total_fan_member', 0)),
            'total_ultimate_mega': int(c.get('total_ultimate_mega', 0)),
            'errors': int(c.get('errors', 0)),
            'retries': int(c.get('retries', 0)),
            'total_lines': int(c.get('total_lines', 0)),
            'threads': int(c.get('threads', 10)),
            'proxy_type': c.get('proxy_type', 'http'),
            'is_running': bool(c.get('is_running', False)),
            'is_paused': bool(c.get('is_paused', False)),
            'timestamp': datetime.now().isoformat(),
        }
        with open(_runtime_state_path(session_id), 'w', encoding='utf-8') as f:
            json.dump(payload, f, indent=2)
    except Exception as e:
        logger.error(f"save_runtime_state failed for {session_id}: {e}")

def load_runtime_state(session_id):
    try:
        p = _runtime_state_path(session_id)
        if not os.path.exists(p):
            return False
        with open(p, 'r', encoding='utf-8') as f:
            s = json.load(f)
        chk.counters.setdefault(session_id, {})
        c = chk.counters[session_id]
        c.update({
            'checked': s.get('checked', 0),
            'invalid': s.get('invalid', 0),
            'hits': s.get('hits', 0),
            'custom': s.get('custom', 0),
            'total_mega_fan': s.get('total_mega_fan', 0),
            'total_fan_member': s.get('total_fan_member', 0),
            'total_ultimate_mega': s.get('total_ultimate_mega', 0),
            'errors': s.get('errors', 0),
            'retries': s.get('retries', 0),
            'total_lines': s.get('total_lines', 0),
            'threads': s.get('threads', 10),
            'proxy_type': s.get('proxy_type', 'http'),
            'is_running': s.get('is_running', False),
            'is_paused': s.get('is_paused', False),
            'completed': False,
            'start_time': datetime.now(),
            'end_time': None,
            'last_pause_time': datetime.now(),
            'total_paused_time': timedelta(0),
            'proxy_lines': chk.counters.get(session_id, {}).get('proxy_lines', []),
        })
        return True
    except Exception as e:
        logger.error(f"load_runtime_state failed for {session_id}: {e}")
        return False


def delete_runtime_state(session_id):
    try:
        p = _runtime_state_path(session_id)
        if os.path.exists(p):
            os.remove(p)
            logger.info(f"Deleted runtime state for {session_id}: {p}")
    except Exception as e:
        logger.error(f"delete_runtime_state failed for {session_id}: {e}")

def dedupe_hits_file(session_id):
    """Remove duplicate lines in hits.txt while preserving order."""
    try:
        session_dir = ensure_session_directory(session_id)
        hit_file_path = os.path.join(session_dir, "hits.txt")
        if not os.path.exists(hit_file_path):
            return 0, 0
        with open(hit_file_path, 'r', encoding='utf-8', errors='ignore') as f:
            lines = [ln.rstrip('\n') for ln in f]
        seen, uniq = set(), []
        for ln in lines:
            key = ln.strip()
            if key and key not in seen:
                seen.add(key)
                uniq.append(ln)
        if len(uniq) != len(lines):
            with open(hit_file_path, 'w', encoding='utf-8') as f:
                f.write('\n'.join(uniq) + ('\n' if uniq else ''))
        return len(lines), len(uniq)
    except Exception as e:
        logger.error(f"dedupe_hits_file failed for {session_id}: {e}")
        return 0, 0
# ==== end helpers ====
app = Flask(__name__)
app.config['SECRET_KEY'] = 'your-secret-key-here-change-this'
socketio = SocketIO(app, cors_allowed_origins="*")


import chk


# ---- BEGIN: baseline helper (inserted) ----
from datetime import datetime, timedelta
import chk

def ensure_counters_baseline(session_id):
    """Ensure chk.counters[session_id] has all required keys; safe to call anytime."""
    try:
        sd = active_sessions.get(session_id, {})
    except NameError:
        sd = {}

    chk.counters.setdefault(session_id, {})
    c = chk.counters[session_id]

    # counters
    c.setdefault('checked', sd.get('checked_on_stop', 0))
    c.setdefault('invalid', 0)
    c.setdefault('hits', 0)
    c.setdefault('custom', 0)
    c.setdefault('total_mega_fan', 0)
    c.setdefault('total_fan_member', 0)
    c.setdefault('total_ultimate_mega', 0)
    c.setdefault('errors', 0)
    c.setdefault('retries', 0)

    # state flags
    c.setdefault('is_running', False)
    c.setdefault('is_paused', False)
    c.setdefault('completed', False)

    # timing
    c.setdefault('start_time', datetime.now())
    c.setdefault('end_time', None)
    c.setdefault('last_pause_time', None)
    c.setdefault('total_paused_time', timedelta(0))

    # session meta
    c.setdefault('total_lines', sd.get('combo_line_count', 0))
    c.setdefault('threads', sd.get('threads', 10))
    c.setdefault('proxy_type', sd.get('proxy_type', 'http'))
    c.setdefault('proxy_lines', c.get('proxy_lines', []))
# ---- END: baseline helper (inserted) ----


active_sessions = {}
# session_proxy_lines will now be managed within chk.counters for better consistency
# and direct access by chk.py functions.
# We'll still use it here temporarily during file upload to pass to chk.py.
session_proxy_lines = {} 

SESSION_DATA_FILE = "session_data.json"
PORT_FILE = "port.txt"
DEFAULT_PORT = 5050

def create_session(existing_session_id=None):
    if existing_session_id:
        session_id = existing_session_id
        session_dir = ensure_session_directory(session_id)
        session_data_path = os.path.join(session_dir, SESSION_DATA_FILE)

        if os.path.exists(session_data_path):
            try:
                with open(session_data_path, 'r', encoding='utf-8') as f:
                    session_data = json.load(f)
                logger.info(f"Loaded existing session data for {session_id}")

                session_data.setdefault('session_id', session_id)
                session_data.setdefault('timestamp', datetime.now().isoformat())
                session_data.setdefault('created_at', time.time())
                session_data.setdefault('combo_file', None)
                session_data.setdefault('proxy_file', None)
                session_data.setdefault('proxy_type', 'http')
                session_data.setdefault('threads', 10)
                session_data.setdefault('is_running', False)
                session_data.setdefault('is_paused', False)
                session_data.setdefault('status_message_id', None)
                session_data.setdefault('current_session_id', session_id)
                session_data.setdefault('combo_line_count', 0)
                session_data.setdefault('proxy_line_count', 0)
                session_data.setdefault('checked_on_stop', 0) # Ensure this key exists

                # Re-read proxy lines if proxy_file exists and update chk.counters directly
                if session_data.get('proxy_file') and os.path.exists(session_data['proxy_file']):
                    try:
                        with open(session_data['proxy_file'], 'r', encoding='utf-8', errors='ignore') as f_proxy:
                            loaded_proxy_lines = [line.strip() for line in f_proxy if line.strip()]
                            session_data['proxy_line_count'] = len(loaded_proxy_lines)
                            # Update chk.counters with these proxy lines
                            if session_id not in chk.counters:
                                chk.counters[session_id] = {} # Initialize if not present
                            chk.counters[session_id]['proxy_lines'] = loaded_proxy_lines
                            logger.info(f"Re-read {session_data['proxy_line_count']} proxy lines for session {session_id} on load.")
                    except Exception as e:
                        logger.error(f"Error re-reading proxy file for session {session_id} on load: {e}")
                        if session_id in chk.counters:
                            chk.counters[session_id]['proxy_lines'] = [] # Clear if error

                if session_data['is_running']:
                    session_data['is_paused'] = True
                    logger.info(f"Auto-paused running checker for session {session_id} on reconnection.")

                    if session_id not in chk.counters:
                        chk.counters[session_id] = {
                            'checked': session_data.get('checked_on_stop', 0), # Load checked count
                            'invalid': 0, 'hits': 0, 'custom': 0,
                            'total_mega_fan': 0, 'total_fan_member': 0, 'total_ultimate_mega': 0,
                            'errors': 0, 'retries': 0,
                            'is_running': False, 'is_paused': False,
                            'completed': False,
                            'start_time': datetime.fromisoformat(session_data['timestamp']),
                            'end_time': None,
                            'total_lines': session_data['combo_line_count'],
                            'last_pause_time': datetime.now(),
                            'total_paused_time': timedelta(0),
                            'threads': session_data['threads'],
                            'proxy_type': session_data['proxy_type'],
                            'proxy_lines': chk.counters.get(session_id, {}).get('proxy_lines', []) # Ensure proxy_lines are loaded
                        }
                        logger.info(f"Re-initialized chk.counters for auto-paused session {session_id}.")
                    else:
                        chk.counters[session_id]['is_paused'] = True
                        chk.counters[session_id]['last_pause_time'] = datetime.now()
                        chk.counters[session_id]['threads'] = session_data['threads']
                        chk.counters[session_id]['proxy_type'] = session_data['proxy_type']
                        # Ensure proxy_lines are updated in chk.counters if they were re-read
                        if 'proxy_lines' in chk.counters.get(session_id, {}):
                            chk.counters[session_id]['proxy_lines'] = chk.counters[session_id]['proxy_lines']
                        logger.info(f"Updated chk.counters for {session_id} to paused.")

                active_sessions[session_id] = session_data
                return session_id, session_data
            except json.JSONDecodeError as e:
                logger.error(f"Error decoding JSON for session {session_id}: {e}. Creating new session data.")
        else:
            logger.info(f"No session_data.json found for {session_id}. Creating new session data.")

    session_id = str(uuid.uuid4())
    session_dir = ensure_session_directory(session_id)

    session_data = {
        'session_id': session_id,
        'timestamp': datetime.now().isoformat(),
        'created_at': time.time(),
        'combo_file': None,
        'proxy_file': None,
        'proxy_type': 'http',
        'threads': 10,
        'is_running': False,
        'is_paused': False,
        'status_message_id': None,
        'current_session_id': session_id,
        'combo_line_count': 0,
        'proxy_line_count': 0,
        'checked_on_stop': 0 # Initialize
    }
    # Initialize chk.counters for new session
    chk.counters[session_id] = {
        'checked': 0, 'invalid': 0, 'hits': 0, 'custom': 0,
        'total_mega_fan': 0, 'total_fan_member': 0, 'total_ultimate_mega': 0,
        'errors': 0, 'retries': 0,
        'is_running': False, 'is_paused': False, 'completed': False,
        'start_time': datetime.now(), 'end_time': None,
        'total_lines': 0, 'last_pause_time': None, 'total_paused_time': timedelta(0),
        'threads': 10, 'proxy_type': 'http', 'proxy_lines': []
    }

    active_sessions[session_id] = session_data
    save_session_data(session_id)
    logger.info(f"Created new session: {session_id}")
    return session_id, session_data

def save_session_data(session_id):
    if session_id not in active_sessions:
        logger.warning(f"Attempted to save non-existent session: {session_id}")
        return

    session_data = active_sessions[session_id]

    # Rehydrate RAM state if missing (post-crash continue)
    try:
        if session_id not in chk.counters or not chk.counters[session_id]:
            if not load_runtime_state(session_id):
                chk.counters[session_id] = {
                    'checked': 0, 'invalid': 0, 'hits': 0, 'custom': 0,
                    'total_mega_fan': 0, 'total_fan_member': 0, 'total_ultimate_mega': 0,
                    'errors': 0, 'retries': 0,
                    'is_running': False, 'is_paused': True, 'completed': False,
                    'start_time': datetime.now(), 'end_time': None,
                    'last_pause_time': datetime.now(), 'total_paused_time': timedelta(0),
                    'threads': active_sessions[session_id].get('threads', 10),
                    'proxy_type': active_sessions[session_id].get('proxy_type', 'http'),
                }
    except Exception:
        pass

    session_dir = ensure_session_directory(session_id)
    session_data_path = os.path.join(session_dir, SESSION_DATA_FILE)

    try:
        with open(session_data_path, 'w', encoding='utf-8') as f:
            json.dump(session_data, f, indent=4)
        logger.debug(f"Saved session data for {session_id}")
    except Exception as e:
        logger.error(f"Failed to save session data for {session_id}: {e}")

def ensure_session_directory(session_id):
    directory = f"session_{session_id}"
    if not os.path.exists(directory):
        os.makedirs(directory)
        logger.info(f"Created session directory: {directory}")
    return directory

def reset_hits_file(session_id):
    session_dir = f"session_{session_id}"
    hit_file_path = f"{session_dir}/hits.txt"
    custom_file_path = f"{session_dir}/custom.txt"

    # Ensure files exist before trying to open them in 'w' mode
    if not os.path.exists(session_dir):
        os.makedirs(session_dir)

    open(hit_file_path, 'w').close()
    open(custom_file_path, 'w').close()
    logger.info(f"Reset hits.txt and custom.txt for session {session_id}")

def clean_session_directory(session_id):
    directory = f"session_{session_id}"
    if not os.path.exists(directory):
        logger.warning(f"Attempted to clean non-existent directory: {directory}")
        return

    logger.info(f"Cleaning session directory: {directory}")

    hits_file = os.path.join(directory, "hits.txt")
    custom_file = os.path.join(directory, "custom.txt")

    backup_dir = os.path.join(directory, "backup")
    if not os.path.exists(backup_dir):
        os.makedirs(backup_dir)
        logger.info(f"Created backup directory: {backup_dir}")

    timestamp = time.strftime("%Y%m%d-%H%M%S")

    if os.path.exists(hits_file) and os.path.getsize(hits_file) > 0:
        backup_file = os.path.join(backup_dir, f"hits_{timestamp}.txt")
        shutil.copy2(hits_file, backup_file)
        logger.info(f"Backed up hits.txt to {backup_file}")

    if os.path.exists(custom_file) and os.path.getsize(custom_file) > 0:
        backup_file = os.path.join(backup_dir, f"custom_{timestamp}.txt")
        shutil.copy2(custom_file, backup_file)
        logger.info(f"Backed up custom.txt to {backup_file}")

    for filename in os.listdir(directory):
        file_path = os.path.join(directory, filename)
        # Exclude backup directory and session_data.json from deletion
        if filename not in ["backup", SESSION_DATA_FILE] and os.path.isfile(file_path):
            try:
                os.remove(file_path)
                logger.debug(f"Removed file: {file_path}")
            except PermissionError:
                logger.warning(f"PermissionError: Could not delete {file_path}. It may be in use.")
            except Exception as e:
                logger.error(f"Error removing file {file_path}: {e}")

@app.route('/crun')
def index():
    return render_template('index.html')


@app.route('/upload_file', methods=['POST'])
def upload_file():
    session_id = request.form.get('session_id')
    file_type = request.form.get('file_type')

    if not session_id or session_id not in active_sessions:
        logger.error(f"File upload failed: Invalid session ID {session_id}")
        return jsonify({'status': 'error', 'message': 'Invalid session ID'}), 400

    session_data = active_sessions[session_id]
    ensure_counters_baseline(session_id)
    # Allow upload if checker is paused, but not if running
    if session_data.get('is_running', False) and not session_data.get('is_paused', False):
        logger.warning(f"Attempted to upload {file_type} while checker is running for session {session_id}")
        return jsonify({'status': 'error', 'message': '❌ UPLOAD FAILED. Stop the checker first.'}), 400

    if 'file' not in request.files:
        logger.error(f"File upload failed for session {session_id}: No file part")
        return jsonify({'status': 'error', 'message': 'No file part'}), 400

    file = request.files['file']
    if file.filename == '':
        logger.error(f"File upload failed for session {session_id}: No selected file")
        return jsonify({'status': 'error', 'message': 'No selected file'}), 400

    session_dir = ensure_session_directory(session_id)
    file_path = os.path.join(session_dir, f"{file_type}.txt")
    line_count = 0

    try:
        file.save(file_path)
        logger.info(f"File '{file.filename}' saved to {file_path} for session {session_id}")

        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            lines = f.readlines()
            if file_type == 'combo':
                valid_lines = [line for line in lines if line.strip() and ':' in line]
                if not valid_lines:
                    os.remove(file_path)
                    return jsonify({'status': 'error', 'message': '❌ NO VALID COMBO! MUST BE AS [email:password].'}), 400
                line_count = len(valid_lines)
            elif file_type == 'proxy':
                valid_lines = [line for line in lines if line.strip()]
                if not valid_lines:
                    os.remove(file_path)
                    return jsonify({'status': 'error', 'message': '❌ No valid proxy lines found! File is empty.'}), 400
                line_count = len(valid_lines)
                # Update chk.counters directly with the new proxy lines
                if session_id not in chk.counters:
                    chk.counters[session_id] = {}
                chk.counters[session_id]['proxy_lines'] = valid_lines

        if file_type == 'combo':
            session_data['combo_file'] = file_path
            session_data['combo_line_count'] = line_count
        elif file_type == 'proxy':
            session_data['proxy_file'] = file_path
            session_data['proxy_line_count'] = line_count
            session_data['proxy_type'] = request.form.get('proxy_type', 'http') # Update proxy type from form
            # Also update proxy_type in chk.counters
            if session_id not in chk.counters:
                chk.counters[session_id] = {}
            chk.counters[session_id]['proxy_type'] = session_data['proxy_type']


        save_session_data(session_id)

        socketio.emit(f'{file_type}_uploaded', {
            'session_id': session_id,
            'count': line_count,
            'message': f'✅ DONE! {line_count} valid lines found for {file_type}.',
            'file_type': file_type,
            'proxy_type': session_data.get('proxy_type')
        }, room=session_id)

        return jsonify({'status': 'success', 'message': f'{file_type.capitalize()} uploaded successfully.'}), 200

    except Exception as e:
        logger.error(f"Error processing {file_type} upload for session {session_id}: {e}", exc_info=True)
        return jsonify({'status': 'error', 'message': f'Error processing {file_type} file: {str(e)}'}), 500


@socketio.on('connect')
def handle_connect():
    logger.info(f"Client connected: {request.sid}. Waiting for session request/reconnect.")
    stats = get_initial_stats()
    emit('stats_update', stats, room=request.sid)

@socketio.on('request_session')
def handle_request_session():
    session_id, session_data = create_session()
    socketio.server.enter_room(request.sid, session_id)
    emit('session_created', {'session_id': session_id}, room=request.sid)
    logger.info(f"New session requested and created: {session_id} for socket {request.sid}")

@socketio.on('reconnect_session')
def handle_reconnect_session(data):
    client_session_id = data.get('session_id')
    if not client_session_id:
        logger.warning(f"Client {request.sid} attempted to reconnect without a session_id.")
        emit('error', {'message': 'No session ID provided for reconnection.'}, room=request.sid)
        session_id, session_data = create_session()
        socketio.server.enter_room(request.sid, session_id)
        emit('session_created', {'session_id': session_id}, room=request.sid)
        return

    session_id, session_data = create_session(existing_session_id=client_session_id)

    # Rehydrate counters & proxies on reconnect
    try:
        if not (session_id in chk.counters and (chk.counters[session_id].get('is_running') or chk.counters[session_id].get('is_paused'))):
            load_runtime_state(session_id)
        pf = session_data.get('proxy_file')
        if pf and os.path.exists(pf):
            chk.counters.setdefault(session_id, {})
            if not chk.counters[session_id].get('proxy_lines'):
                with open(pf, 'r', encoding='utf-8', errors='ignore') as f_proxy:
                    chk.counters[session_id]['proxy_lines'] = [ln.strip() for ln in f_proxy if ln.strip()]
    except Exception as _e:
        logger.error(f"reconnect rehydrate failed: {_e}")


    if session_id == client_session_id:
        socketio.server.enter_room(request.sid, session_id)
        logger.info(f"Client {request.sid} reconnected to existing session: {session_id}")

        current_stats = get_current_stats(session_id)

        previous_state = {
            'stats': current_stats,
            'combo_file_uploaded': session_data.get('combo_file') is not None and os.path.exists(session_data.get('combo_file', '')),
            'proxy_file_uploaded': session_data.get('proxy_file') is not None and os.path.exists(session_data.get('proxy_file', '')),
            'threads': session_data.get('threads', 10),
            'proxy_type': session_data.get('proxy_type', 'http'),
            'checker_status': 'paused' if session_data['is_paused'] else ('running' if session_data['is_running'] else 'stopped')
        }

        emit('session_reconnected', {'session_id': session_id, 'previous_state': previous_state}, room=request.sid)

        if session_data.get('is_running', False):
            logger.info(f"Resuming status update thread for auto-paused session {session_id}")
            if not hasattr(socketio, 'status_threads'):
                socketio.status_threads = {}
            if session_id not in socketio.status_threads or not socketio.status_threads[session_id].is_alive():
                status_thread = threading.Thread(
                    target=update_status_websocket,
                    args=(session_id,)
                )
                status_thread.daemon = True
                status_thread.start()
                socketio.status_threads[session_id] = status_thread
            else:
                logger.info(f"Status update thread for {session_id} already running.")

    else:
        logger.warning(f"Client {request.sid} provided invalid session ID {client_session_id}. Created new session: {session_id}")
        socketio.server.enter_room(request.sid, session_id)
        emit('session_created', {'session_id': session_id}, room=request.sid)


@socketio.on('upload_combo')
def handle_combo_upload(data):

    # Mark that new inputs were uploaded; next Start should reset stats
    try:
        active_sessions[session_id]['pending_reset'] = True
    except Exception:
        pass

    session_id = data['session_id']
    combo_content = data['content']

    if session_id not in active_sessions:
        emit('error', {'message': 'Invalid session'}, room=request.sid)
        return

    session_data = active_sessions[session_id]
    # Allow upload if checker is paused, but not if running
    if session_data.get('is_running', False) and not session_data.get('is_paused', False):
        emit('error', {'message': '❌ FAILED WHILE UPLOADING. Stop the checker first.'}, room=request.sid)
        return

    session_dir = ensure_session_directory(session_id)
    combo_file = os.path.join(session_dir, "combo.txt")

    lines = combo_content.strip().split('\n')
    valid_lines = [line for line in lines if line.strip() and ':' in line]

    if not valid_lines:
        emit('error', {'message': '❌ NO VALID COMBO! MUST BE AS [email:password].'}, room=request.sid)
        return

    try:
        with open(combo_file, 'w', encoding='utf-8') as f:
            f.write('\n'.join(valid_lines))

        session_data['combo_file'] = combo_file
        session_data['combo_line_count'] = len(valid_lines)
        save_session_data(session_id)
        emit('combo_uploaded', {'count': len(valid_lines), 'message': f'✅ DONE! {len(valid_lines)} valid lines found.', 'file_type': 'combo'}, room=session_id)
        logger.info(f"Combo uploaded for session {session_id}: {len(valid_lines)} lines.")
    except Exception as e:
        emit('error', {'message': f'Error saving combo file: {str(e)}'}, room=request.sid)
        logger.error(f"Error saving combo file for {session_id}: {e}")


@socketio.on('upload_proxy')
def handle_proxy_upload(data):

    # Mark that new inputs were uploaded; next Start should reset stats
    try:
        active_sessions[session_id]['pending_reset'] = True
    except Exception:
        pass

    session_id = data['session_id']
    proxy_content = data['content']
    proxy_type = data.get('proxy_type', 'http')

    if session_id not in active_sessions:
        emit('error', {'message': 'Invalid session'}, room=request.sid)
        return

    session_data = active_sessions[session_id]
    ensure_counters_baseline(session_id)
    # Allow upload if checker is paused, but not if running
    if session_data.get('is_running', False) and not session_data.get('is_paused', False):
        emit('error', {'message': '❌ UPLOAD FAILED. Stop the checker first.'}, room=request.sid)
        return

    session_dir = ensure_session_directory(session_id)
    proxy_file = os.path.join(session_dir, "proxy.txt")

    lines = proxy_content.strip().split('\n')
    valid_lines = [line for line in lines if line.strip()]

    if not valid_lines:
        emit('error', {'message': '❌ No valid proxy lines found! File is empty.'}, room=request.sid)
        return

    try:
        with open(proxy_file, 'w', encoding='utf-8') as f:
            f.write('\n'.join(valid_lines))

        session_data['proxy_file'] = proxy_file
        session_data['proxy_type'] = proxy_type
        session_data['proxy_line_count'] = len(valid_lines)
        save_session_data(session_id)
        
        # Update chk.counters directly with the new proxy lines and type
        if session_id not in chk.counters:
            chk.counters[session_id] = {}
        chk.counters[session_id]['proxy_lines'] = valid_lines
        chk.counters[session_id]['proxy_type'] = proxy_type

        emit('proxy_uploaded', {'count': len(valid_lines), 'type': proxy_type, 'message': f'✅ DONE! {len(valid_lines)} valid lines found.', 'file_type': 'proxy', 'proxy_type': proxy_type}, room=session_id)
        logger.info(f"Proxy uploaded for session {session_id}: {len(valid_lines)} lines, type {proxy_type}.")
    except Exception as e:
        emit('error', {'message': f'Error saving proxy file: {str(e)}'}, room=request.sid)
        logger.error(f"Error saving proxy file for {session_id}: {e}")


@socketio.on('start_checker')
def handle_start_checker(data):
    session_id = data.get('session_id')
    if not session_id:
        emit('error', {'message': 'Invalid session.'}, room=request.sid)
        return


    # If user uploaded new inputs before starting, reset stats and files
    try:
        if active_sessions[session_id].get('pending_reset'):
            reset_for_new_inputs(session_id, active_sessions[session_id])
            active_sessions[session_id]['pending_reset'] = False
    except Exception as _e:
        logger.error(f"reset_for_new_inputs failed: {_e}")
    threads = data.get('threads', 10)
    initial_progress_line = data.get('initial_progress_line', 0)
    proxy_type = data.get('proxy_type', 'http') # Get proxy_type from data

    if not session_id or session_id not in active_sessions:
        emit('error', {'message': 'Invalid session'}, room=request.sid)
        return

    session_data = active_sessions[session_id]


    # Guard: don't start if already running; if paused, ask to Continue
    is_running_backend = session_data.get('is_running', False) and not session_data.get('is_paused', False)
    is_running_chk = (session_id in chk.counters and chk.counters[session_id].get('is_running', False) and not chk.counters[session_id].get('is_paused', False))
    if is_running_backend or is_running_chk:
        emit('error', {'message': '❌ Checker is already running. Pause or stop it first.'}, room=session_id)
        return
    if session_data.get('is_paused', False) or (session_id in chk.counters and chk.counters[session_id].get('is_paused', False)):
        emit('error', {'message': '⏸️ Checker is paused. Use Continue to resume.'}, room=session_id)
        return
    if session_data.get('is_running', False) and not session_data.get('is_paused', False):
        emit('error', {'message': '❌ Checker is already running. Please stop it first.'}, room=request.sid)
        return

    combo_file = session_data.get('combo_file')
    if not combo_file or not os.path.exists(combo_file) or os.path.getsize(combo_file) == 0:
        emit('error', {'message': '❌ MISSING COMBO FILE! Please upload a combo file first.'}, room=request.sid)
        return

    proxy_file = session_data.get('proxy_file')
    if not proxy_file or not os.path.exists(proxy_file) or os.path.getsize(proxy_file) == 0:
        emit('error', {'message': '❌ MISSING PROXY FILE! Please upload a proxy file first.'}, room=request.sid)
        return

    try:
        with open(combo_file, 'r', encoding='utf-8', errors='ignore') as f:
            combo_content = f.read().strip()
            if not combo_content or not any(':' in line for line in combo_content.splitlines()):
                emit('error', {'message': '❌ Invalid combo file! File must contain lines in format email:password.'}, room=request.sid)
                return
    except Exception as e:
        emit('error', {'message': f'❌ Error reading combo file: {str(e)}'}, room=request.sid)
        return

    try:
        with open(proxy_file, 'r', encoding='utf-8', errors='ignore') as f:
            proxy_content = f.read().strip()
            if not proxy_content:
                emit('error', {'message': '❌ Invalid proxy file! File is empty.'}, room=request.sid)
                return
            # Update chk.counters directly with the proxy lines
            if session_id not in chk.counters:
                chk.counters[session_id] = {}
            chk.counters[session_id]['proxy_lines'] = proxy_content.splitlines()
    except Exception as e:
        emit('error', {'message': f'❌ Error reading proxy file: {str(e)}'}, room=request.sid)
        return

    if not (1 <= threads <= 400):
        emit('error', {'message': '❌ Thread count must be between 1 and 400.'}, room=request.sid)
        return

    # Update session_data with current threads and proxy_type from UI
    session_data['threads'] = threads
    session_data['proxy_type'] = proxy_type

    # Update chk.counters with current threads and proxy_type
    if session_id not in chk.counters:
        chk.counters[session_id] = {}
    chk.counters[session_id]['threads'] = threads
    chk.counters[session_id]['proxy_type'] = proxy_type
    # Reset timing baselines for a fresh start
    ensure_counters_baseline(session_id)
    chk.counters[session_id]['start_time'] = datetime.now()
    chk.counters[session_id]['end_time'] = None
    chk.counters[session_id]['total_paused_time'] = timedelta(0)
    chk.counters[session_id]['last_pause_time'] = None

    # Reset hits/custom files only if starting fresh, not if continuing from a paused state
    # The progress line setting will handle resetting stats if needed
    if not session_data.get('is_paused', False):
        reset_hits_file(session_id)

    # Initialize or update chk.counters for a fresh start or if not previously paused
    # This logic is now mostly handled by chk.start_checker itself, but we ensure
    # the basic state is set here for app.py's tracking.
    session_data['is_running'] = True
    session_data['is_paused'] = False
    try:
        save_runtime_state(session_id)
    except Exception:
        pass
    save_session_data(session_id) # Save updated session_data

    if not hasattr(socketio, 'checker_threads'):
        socketio.checker_threads = {}

    # If checker is already running (e.g., paused), signal chk.py to restart its executor
    if session_id in socketio.checker_threads and socketio.checker_threads[session_id].is_alive():
        logger.info(f"Checker thread for session {session_id} is already running. Signaling for restart with new settings.")
        if session_id in chk.counters:
            chk.counters[session_id]['_restart_signal'] = True # Signal chk.py to restart its executor
            chk.counters[session_id]['is_paused'] = False # Unpause it
            chk.counters[session_id]['is_running'] = True # Mark as running
            chk.counters[session_id]['threads'] = threads # Update threads
            chk.counters[session_id]['proxy_type'] = proxy_type # Update proxy type
            chk.counters[session_id]['checked'] = initial_progress_line # Update checked count
            # Reset start time and paused time if it's a fresh start or significant jump
            if initial_progress_line == 0 or chk.counters[session_id].get('start_time') is None:
                chk.counters[session_id]['start_time'] = datetime.now()
                chk.counters[session_id]['total_paused_time'] = timedelta(0)
            elif chk.counters[session_id]['last_pause_time']:
                pause_duration = datetime.now() - chk.counters[session_id]['last_pause_time']
                chk.counters[session_id]['total_paused_time'] += pause_duration
                chk.counters[session_id]['last_pause_time'] = None
            
            # Reset stats if progress line changed or it's a fresh start
            if chk.counters[session_id]['checked'] != initial_progress_line or initial_progress_line == 0:
                chk.counters[session_id]['invalid'] = 0
                chk.counters[session_id]['hits'] = 0
                chk.counters[session_id]['custom'] = 0
                chk.counters[session_id]['total_mega_fan'] = 0
                chk.counters[session_id]['total_fan_member'] = 0
                chk.counters[session_id]['total_ultimate_mega'] = 0
                chk.counters[session_id]['errors'] = 0
                chk.counters[session_id]['retries'] = 0
                logger.info(f"Session {session_id}: Stats reset due to restart with new progress line.")

            # Ensure status update thread is running
            if not hasattr(socketio, 'status_threads'):
                socketio.status_threads = {}
            if session_id not in socketio.status_threads or not socketio.status_threads[session_id].is_alive():
                status_thread = threading.Thread(
                    target=update_status_websocket,
                    args=(session_id,)
                )
                status_thread.daemon = True
                status_thread.start()
                socketio.status_threads[session_id] = status_thread
        else:
            logger.error(f"chk.counters for session {session_id} not found during restart signal.")
            emit('error', {'message': '❌ Internal error: Checker state not found.'}, room=request.sid)
            return
    else:
        # Start a new checker thread if none is running
        checker_thread = threading.Thread(
            target=start_checker_process,
            args=(session_id, combo_file, chk.counters[session_id]['proxy_lines'], threads, proxy_type, initial_progress_line)
        )
        checker_thread.daemon = True
        checker_thread.start()
        socketio.checker_threads[session_id] = checker_thread

    emit('checker_started', {'message': '✅ Checker started successfully!'}, room=session_id)
    logger.info(f"Checker started for session {session_id} with {threads} threads, starting at line {initial_progress_line}.")


@socketio.on('stop_checker')
def handle_stop_checker(data):
    session_id = data['session_id']

    if session_id not in active_sessions:
        emit('error', {'message': 'Invalid session'}, room=request.sid)
        return

    session_data = active_sessions[session_id]

    is_active_in_backend = session_data.get('is_running', False)
    is_active_in_chk = session_id in chk.counters and chk.counters[session_id]['is_running']

    if not (is_active_in_backend or is_active_in_chk):
        emit('error', {'message': '❌ No active checking process to stop.'}, room=request.sid)
        return

    if session_id in chk.counters:
        chk.counters[session_id]['is_running'] = False
        chk.counters[session_id]['completed'] = False
        chk.counters[session_id]['end_time'] = datetime.now()
        chk.counters[session_id]['is_paused'] = False
        chk.counters[session_id]['last_pause_time'] = None
        chk.counters[session_id]['_restart_signal'] = False # Clear any restart signal
        logger.info(f"Signaled chk.py to stop for session {session_id}.")

    session_data['is_running'] = False
    session_data['is_paused'] = False
    # Store the final checked count when checker stops
    if session_id in chk.counters:
        session_data['checked_on_stop'] = chk.counters[session_id]['checked']
    save_session_data(session_id)

    time.sleep(1) # Give checker thread a moment to recognize stop signal

    hits_file = os.path.join(ensure_session_directory(session_id), "hits.txt")
    if os.path.exists(hits_file) and os.path.getsize(hits_file) > 0:
        try:
            with open(hits_file, 'r', encoding='utf-8') as f:
                content = f.read()

            emit('hits_available', {
                'content': content,
                'filename': f'hits_{session_id}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.txt'
            }, room=session_id)
        except Exception as e:
            logger.error(f"Error reading hits file on stop for {session_id}: {e}")
            emit('error', {'message': f'Error reading hits file: {str(e)}'}, room=request.sid)

    clean_session_directory(session_id)


    # Clear in-memory stats so next run starts clean ## CLEAR_AFTER_STOP
    if session_id in chk.counters:
        chk.counters[session_id].update({
            'checked': 0, 'invalid': 0, 'hits': 0, 'custom': 0,
            'total_mega_fan': 0, 'total_fan_member': 0, 'total_ultimate_mega': 0,
            'errors': 0, 'retries': 0,
            'is_running': False, 'is_paused': False, 'completed': False,
            'start_time': datetime.now(), 'end_time': None,
            'last_pause_time': None, 'total_paused_time': timedelta(0)
        })
    if session_id in active_sessions:
        active_sessions[session_id]['checked_on_stop'] = 0
        active_sessions[session_id]['is_running'] = False
        active_sessions[session_id]['is_paused'] = False
        save_session_data(session_id)
    emit('checker_stopped', {'message': '✅ CHECKER HAS BEEN STOPPED.'}, room=session_id)
    # Push a definitive STOPPED snapshot so clients never see PAUSED after clearing
    try:
        socketio.emit('stats_update', {
            'status': '❌ STOPPED',
            'total_lines': 0,
            'checked': 0,
            'invalid': 0,
            'hits': 0,
            'custom': 0,
            'total_mega_fan': 0,
            'total_fan_member': 0,
            'total_ultimate_mega': 0,
            'errors': 0,
            'retries': 0,
            'cpm': 0,
            'elapsed_time': '0:00:00'
        }, room=session_id)
    except Exception:
        pass

    # Tell client to wipe inputs immediately
    socketio.emit('inputs_cleared', {}, room=session_id)

    logger.info(f"Checker stopped for session {session_id}.")



    # Make sure we don't wipe stats; just mark stopped
    try:
        if session_id in chk.counters:
            c = chk.counters[session_id]
            c['is_running'] = False
            c['is_paused'] = False
            c['completed'] = False
            c['end_time'] = datetime.now()
    except Exception:
        pass

    # Dedupe hits and snapshot for safe resume
    try:
        before, after = dedupe_hits_file(session_id)
        logger.info(f"hits.txt deduped for {session_id}: {before} -> {after}")
    except Exception:
        pass

    try:
        delete_runtime_state(session_id)
    except Exception:
        pass

    emit('status_message', {'type': 'info', 'message': '🛑 Stopped checker.'}, room=session_id)
    socketio.emit('stats_update', chk.generate_stats_text(session_id), room=session_id)

@socketio.on('pause_checker')
def handle_pause_checker(data):
    session_id = data['session_id']

    if session_id not in active_sessions:
        emit('error', {'message': 'Invalid session'}, room=request.sid)
        return

    session_data = active_sessions[session_id]

    is_running_in_chk = session_id in chk.counters and chk.counters[session_id]['is_running']
    is_running_in_session_data = session_data.get('is_running', False)

    if not (is_running_in_chk or is_running_in_session_data):
        emit('error', {'message': '❌ No active checker to pause. Start one first.'}, room=request.sid)
        return

    if session_data.get('is_paused', False):
        emit('error', {'message': '❌ Checker is already paused.'}, room=request.sid)
        return

    if is_running_in_chk:
        chk.counters[session_id]['is_paused'] = True
        chk.counters[session_id]['last_pause_time'] = datetime.now()
        chk.counters[session_id]['_restart_signal'] = False # Clear any restart signal
        logger.info(f"Checker paused in chk.counters for session {session_id}.")
    else:
        # This block handles cases where session_data says running but chk.counters doesn't exist or isn't running.
        # This can happen if the backend restarted or chk.py state was lost.
        # We try to re-initialize chk.counters to a paused state based on session_data.
        if session_data.get('combo_file') and session_data.get('proxy_file'):
            # Ensure chk.counters exists and has proxy_lines if re-initializing
            if session_id not in chk.counters:
                chk.counters[session_id] = {}
            if 'proxy_lines' not in chk.counters[session_id]:
                try:
                    with open(session_data['proxy_file'], 'r', encoding='utf-8', errors='ignore') as f_proxy:
                        chk.counters[session_id]['proxy_lines'] = [line.strip() for line in f_proxy if line.strip()]
                except Exception as e:
                    logger.error(f"Error re-reading proxy file for re-initializing chk.counters on pause: {e}")
                    chk.counters[session_id]['proxy_lines'] = []

            chk.counters[session_id].update({
                'checked': session_data.get('checked_on_stop', 0), # Use last checked count
                'invalid': 0, 'hits': 0, 'custom': 0,
                'total_mega_fan': 0, 'total_fan_member': 0, 'total_ultimate_mega': 0,
                'errors': 0, 'retries': 0,
                'is_running': True, # Mark as running but immediately paused
                'is_paused': True,
                'completed': False,
                'start_time': datetime.fromisoformat(session_data['timestamp']),
                'end_time': None,
                'total_lines': session_data['combo_line_count'],
                'last_pause_time': datetime.now(),
                'total_paused_time': timedelta(0),
                'threads': session_data['threads'],
                'proxy_type': session_data['proxy_type'],
                '_restart_signal': False
            })
            logger.info(f"Re-initialized chk.counters for session {session_id} to paused state during pause request.")
        else:
            emit('error', {'message': '❌ Cannot pause: Checker state is ambiguous or files are missing.'}, room=request.sid)
            logger.warning(f"Attempted to pause session {session_id} but chk.counters missing and session_data incomplete.")
            return

    session_data['is_paused'] = True
    save_session_data(session_id)

    emit('checker_paused', {'message': '⏸ PAUSED. Use Continue to resume.'}, room=session_id)
    logger.info(f"Checker paused for session {session_id}.")


@socketio.on('continue_checker')
def handle_continue_checker(data):
    session_id = data['session_id']

    if session_id not in active_sessions:
        emit('error', {'message': 'Invalid session'}, room=request.sid)
        return

    session_data = active_sessions[session_id]

    # Get current threads and proxy type from the data sent by the client (UI)
    current_threads = data.get('threads', session_data.get('threads', 10))
    current_proxy_type = data.get('proxy_type', session_data.get('proxy_type', 'http'))

    is_running_in_chk = session_id in chk.counters and chk.counters[session_id]['is_running']
    is_paused_in_chk = session_id in chk.counters and chk.counters[session_id]['is_paused']
    is_running_in_session_data = session_data.get('is_running', False)
    is_paused_in_session_data = session_data.get('is_paused', False)

    if not (is_running_in_chk or is_running_in_session_data):
        emit('error', {'message': '❌ No active checker to continue. Start one first.'}, room=request.sid)
        return
    if not (is_paused_in_chk or is_paused_in_session_data):
        emit('error', {'message': '❌ Checker is not paused.'}, room=request.sid)
        return

    proxy_file = session_data.get('proxy_file')
    if proxy_file and os.path.exists(proxy_file):
        try:
            with open(proxy_file, 'r', encoding='utf-8', errors='ignore') as f:
                proxy_content = f.read().strip()
                if not proxy_content:
                    emit('error', {'message': '❌ Proxy file is now empty! Cannot continue.'}, room=request.sid)
                    return
                # Update chk.counters directly with the new proxy lines
                if session_id not in chk.counters:
                    chk.counters[session_id] = {}
                chk.counters[session_id]['proxy_lines'] = proxy_content.splitlines()
                logger.info(f"Re-read proxy file for session {session_id} on continue.")
        except Exception as e:
            logger.error(f"Error re-reading proxy file for session {session_id} on continue: {e}")
            emit('error', {'message': f'❌ Error re-reading proxy file: {str(e)}'}, room=request.sid)
            return
    else:
        emit('error', {'message': '❌ Proxy file missing or inaccessible. Cannot continue.'}, room=request.sid)
        return

    # Update session_data with current threads and proxy_type from UI
    session_data['threads'] = current_threads
    session_data['proxy_type'] = current_proxy_type

    # Update chk.counters with current threads and proxy_type
    if session_id not in chk.counters:
        chk.counters[session_id] = {}
    chk.counters[session_id]['threads'] = current_threads
    chk.counters[session_id]['proxy_type'] = current_proxy_type

    # Signal chk.py to restart its executor with new settings
    if session_id in chk.counters:
        chk.counters[session_id]['_restart_signal'] = True
        chk.counters[session_id]['is_paused'] = False
        chk.counters[session_id]['is_running'] = True
        if chk.counters[session_id]['last_pause_time']:
            pause_duration = datetime.now() - chk.counters[session_id]['last_pause_time']
            chk.counters[session_id]['total_paused_time'] += pause_duration
            chk.counters[session_id]['last_pause_time'] = None
        logger.info(f"Signaled chk.py to restart executor for session {session_id} on continue.")
    else:
        # This block handles cases where session_data says paused but chk.counters doesn't exist.
        # We try to re-initialize chk.counters to a running state based on session_data.
        if session_data.get('combo_file') and session_data.get('proxy_file'):
            current_checked_count = session_data.get('checked_on_stop', 0) # Use last checked count
            total_paused_time_preserved = timedelta(0)
            if session_id in chk.counters: # This check is redundant if we're in the 'else' block, but safe.
                current_checked_count = chk.counters[session_id]['checked']
                total_paused_time_preserved = chk.counters[session_id].get('total_paused_time', timedelta(0))

            # Ensure chk.counters exists and has proxy_lines if re-initializing
            if session_id not in chk.counters:
                chk.counters[session_id] = {}
            if 'proxy_lines' not in chk.counters[session_id]:
                try:
                    with open(session_data['proxy_file'], 'r', encoding='utf-8', errors='ignore') as f_proxy:
                        chk.counters[session_id]['proxy_lines'] = [line.strip() for line in f_proxy if line.strip()]
                except Exception as e:
                    logger.error(f"Error re-reading proxy file for re-initializing chk.counters on continue: {e}")
                    chk.counters[session_id]['proxy_lines'] = []

            chk.counters[session_id].update({
                'checked': current_checked_count,
                'invalid': 0, 'hits': 0, 'custom': 0,
                'total_mega_fan': 0, 'total_fan_member': 0, 'total_ultimate_mega': 0,
                'errors': 0, 'retries': 0,
                'is_running': True,
                'is_paused': False,
                'completed': False,
                'start_time': datetime.fromisoformat(session_data['timestamp']),
                'end_time': None,
                'total_lines': session_data['combo_line_count'],
                'last_pause_time': None,
                'total_paused_time': total_paused_time_preserved,
                'threads': current_threads,
                'proxy_type': current_proxy_type,
                '_restart_signal': True # Signal for restart
            })
            logger.info(f"Re-initialized chk.counters for session {session_id} to running state during continue request.")

            # Start a new checker thread if none is running
            if not hasattr(socketio, 'checker_threads'):
                socketio.checker_threads = {}
            if session_id not in socketio.checker_threads or not socketio.checker_threads[session_id].is_alive():
                checker_thread = threading.Thread(
                    target=start_checker_process,
                    args=(session_id, session_data['combo_file'], chk.counters[session_id]['proxy_lines'], current_threads, current_proxy_type, current_checked_count)
                )
                checker_thread.daemon = True
                checker_thread.start()
                socketio.checker_threads[session_id] = checker_thread
            else:
                logger.info(f"Checker thread for {session_id} already running, just unpausing and signaling restart.")

            if not hasattr(socketio, 'status_threads'):
                socketio.status_threads = {}
            if session_id not in socketio.status_threads or not socketio.status_threads[session_id].is_alive():
                status_thread = threading.Thread(
                    target=update_status_websocket,
                    args=(session_id,)
                )
                status_thread.daemon = True
                status_thread.start()
                socketio.status_threads[session_id] = status_thread
            else:
                logger.info(f"Status update thread for {session_id} already running.")
        else:
            emit('error', {'message': '❌ Cannot continue: Checker state is ambiguous or files are missing.'}, room=request.sid)
            logger.warning(f"Attempted to continue session {session_id} but chk.counters missing and session_data incomplete.")
            return

    session_data['is_paused'] = False
    save_session_data(session_id)

    emit('checker_continued', {'message': '▶️ Checker has been resumed.'}, room=session_id)
    logger.info(f"Checker continued for session {session_id}.")

@socketio.on('download_hits')
def handle_download_hits(data):
    session_id = data['session_id']

    if session_id not in active_sessions:
        emit('error', {'message': 'Invalid session'}, room=request.sid)
        return

    hits_file = os.path.join(ensure_session_directory(session_id), "hits.txt")

    if not os.path.exists(hits_file) or os.path.getsize(hits_file) == 0:
        emit('error', {'message': '❌ No hits found!'}, room=request.sid)
        return

    try:
        with open(hits_file, 'r', encoding='utf-8') as f:
            content = f.read()

        emit('hits_download', {
            'content': content,
            'filename': f'hits_{session_id}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.txt'
        }, room=session_id)
        logger.info(f"Hits downloaded for session {session_id}.")
    except Exception as e:
        emit('error', {'message': f'Error reading hits file: {str(e)}'}, room=request.sid)
        logger.error(f"Error reading hits file for download {session_id}: {e}")

@socketio.on('set_progress_line')
def handle_set_progress_line(data):
    session_id = data['session_id']
    new_progress_line = data['progress_line']

    if session_id not in active_sessions:
        emit('error', {'message': 'Invalid session'}, room=request.sid)
        return

    session_data = active_sessions[session_id]

    # Allow setting progress line if checker is paused or stopped
    if session_data.get('is_running', False) and not session_data.get('is_paused', False):
        emit('error', {'message': '❌ Cannot set progress while checker is running. Pause or stop it first.'}, room=request.sid)
        return

    total_lines = session_data.get('combo_line_count', 0)
    if new_progress_line > total_lines:
        emit('error', {'message': f'❌ Progress line cannot exceed total combo lines ({total_lines}).'}, room=request.sid)
        return
    if new_progress_line < 0:
        emit('error', {'message': '❌ Progress line cannot be negative.'}, room=request.sid)
        return

    # Ensure chk.counters exists for the session
    if session_id not in chk.counters:
        chk.counters[session_id] = {
            'checked': 0, 'invalid': 0, 'hits': 0, 'custom': 0,
            'total_mega_fan': 0, 'total_fan_member': 0, 'total_ultimate_mega': 0,
            'errors': 0, 'retries': 0,
            'is_running': False, 'is_paused': False, 'completed': False,
            'start_time': datetime.now(), 'end_time': None,
            'total_lines': total_lines, 'last_pause_time': None, 'total_paused_time': timedelta(0),
            'threads': session_data.get('threads', 10), 'proxy_type': session_data.get('proxy_type', 'http'),
            'proxy_lines': chk.counters.get(session_id, {}).get('proxy_lines', []) # Preserve proxy lines if they exist
        }

    # Reset stats if progress line is changed
    if chk.counters[session_id]['checked'] != new_progress_line:
        chk.counters[session_id]['checked'] = new_progress_line
        chk.counters[session_id]['invalid'] = 0
        chk.counters[session_id]['hits'] = 0
        chk.counters[session_id]['custom'] = 0
        chk.counters[session_id]['total_mega_fan'] = 0
        chk.counters[session_id]['total_fan_member'] = 0
        chk.counters[session_id]['total_ultimate_mega'] = 0
        chk.counters[session_id]['errors'] = 0
        chk.counters[session_id]['retries'] = 0
        
        # Reset start time and paused time to reflect a new "start" for stats
        chk.counters[session_id]['start_time'] = datetime.now()
        chk.counters[session_id]['total_paused_time'] = timedelta(0)
        chk.counters[session_id]['last_pause_time'] = None # Clear last pause time
        
        logger.info(f"Session {session_id}: Progress line set to {new_progress_line}. Stats reset.")
    else:
        logger.info(f"Session {session_id}: Progress line already at {new_progress_line}. No change needed.")

    # Emit updated stats to reflect the new progress line and potentially reset counters
    socketio.emit('stats_update', chk.generate_stats_text(session_id), room=session_id)
    emit('progress_line_updated', {'message': f'✅ Progress line set to {new_progress_line}.', 'new_checked_count': new_progress_line}, room=session_id)


def get_initial_stats():
    return {
        'status': '❌ STOPPED',
        'total_lines': 0,
        'checked': 0,
        'invalid': 0,
        'hits': 0,
        'custom': 0,
        'total_mega_fan': 0,
        'total_fan_member': 0,
        'total_ultimate_mega': 0,
        'errors': 0,
        'retries': 0,
        'cpm': 0,
        'elapsed_time': '0:00:00'
    }

def get_current_stats(session_id):
    if session_id in chk.counters:
        return chk.generate_stats_text(session_id)
    else:
        session_data = active_sessions.get(session_id)
        if session_data:
            stats = get_initial_stats()
            stats['total_lines'] = session_data.get('combo_line_count', 0)
            stats['checked'] = session_data.get('checked_on_stop', 0) # Use checked_on_stop for display
            
            if session_data.get('is_running', False):
                if session_data.get('is_paused', False):
                    stats['status'] = '⏸️ PAUSED'
                else:
                    stats['status'] = '🔄 RUNNING'
            return stats
        return get_initial_stats()

def start_checker_process(session_id, combo_file, proxy_lines, threads, proxy_type, initial_progress_line=0):
    try:
        if not hasattr(socketio, 'status_threads'):
            socketio.status_threads = {}
        if session_id not in socketio.status_threads or not socketio.status_threads[session_id].is_alive():
            status_thread = threading.Thread(
                target=update_status_websocket,
                args=(session_id,)
            )
            status_thread.daemon = True
            status_thread.start()
            socketio.status_threads[session_id] = status_thread
        else:
            logger.info(f"Status update thread for {session_id} already running.")

        # Pass the combo_file path directly, chk.py will read it.
        # chk.py will now get proxy_lines and proxy_type from its own chk.counters[session_id]
        chk.start_checker(session_id, combo_file, threads, socketio, initial_progress_line)

    except Exception as e:
        socketio.emit('error', {'message': f'Checker error: {str(e)}'}, room=session_id)
        logger.critical(f"Critical checker error for session {session_id}: {e}", exc_info=True)
    finally:
        if session_id in active_sessions:
            session_data = active_sessions[session_id]

            if not session_data['is_paused']:
                session_data['is_running'] = False
                session_data['is_paused'] = False
                # Store the final checked count when checker stops
                if session_id in chk.counters:
                    session_data['checked_on_stop'] = chk.counters[session_id]['checked']
                save_session_data(session_id)
                logger.info(f"Checker process finished for session {session_id}. Marked as stopped.")
            else:
                logger.info(f"Checker process finished for session {session_id} but left paused.")

        if hasattr(socketio, 'checker_threads') and session_id in socketio.checker_threads:
            del socketio.checker_threads[session_id]

        # Only stop status thread if checker is truly stopped (not paused)
        if hasattr(socketio, 'status_threads') and session_id in socketio.status_threads:
            if session_id in chk.counters and not chk.counters[session_id]['is_running'] and not chk.counters[session_id]['is_paused']:
                del socketio.status_threads[session_id]


def update_status_websocket(session_id):
    logger.info(f"Starting status update thread for session {session_id}")
    while session_id in active_sessions and (active_sessions[session_id]['is_running'] or active_sessions[session_id]['is_paused']):
        state = chk.counters.get(session_id, {})
        if not state or (not state.get('is_running', False) and not state.get('is_paused', False)):
            logger.info(f"Checker process for {session_id} is no longer running or paused in chk.counters. Stopping status update.")
            if session_id in active_sessions:
                session_data = active_sessions[session_id]
                if not session_data['is_paused']:
                    session_data['is_running'] = False
                    session_data['is_paused'] = False
                    # Store the final checked count when checker stops
                    if session_id in chk.counters:
                        session_data['checked_on_stop'] = chk.counters[session_id]['checked']
                    save_session_data(session_id)
            break

        stats = chk.generate_stats_text(session_id)
        socketio.emit('stats_update', stats, room=session_id)

        if chk.counters[session_id]['completed'] and not chk.counters[session_id]['is_running']:
            logger.info(f"All lines checked for session {session_id}. Marking as completed.")

            chk.counters[session_id]['end_time'] = datetime.now()

            if session_id in active_sessions:
                session_data = active_sessions[session_id]
                session_data['is_running'] = False
                session_data['is_paused'] = False
                session_data['checked_on_stop'] = chk.counters[session_id]['checked'] # Store final checked count
                save_session_data(session_id)

            hits_file = os.path.join(ensure_session_directory(session_id), "hits.txt")
            if os.path.exists(hits_file) and os.path.getsize(hits_file) > 0:
                try:
                    with open(hits_file, 'r', encoding='utf-8') as f:
                        content = f.read()

                    socketio.emit('hits_available', {
                        'content': content,
                        'filename': f'hits_{session_id}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.txt'
                    }, room=session_id)
                    logger.info(f"Hits available for completed session {session_id}.")
                except Exception as e:
                    logger.error(f"Error reading hits file for completed session {session_id}: {e}")
                    socketio.emit('error', {'message': f'Error reading hits file: {str(e)}'}, room=session_id)

            clean_session_directory(session_id)

            socketio.emit('checker_completed', {'message': '✅ COMPLETE!'}, room=session_id)
            break

        time.sleep(2)

    logger.info(f"Status update thread for session {session_id} has stopped.")

    if hasattr(socketio, 'status_threads') and session_id in socketio.status_threads:
        if session_id in chk.counters and not chk.counters[session_id]['is_running'] and not chk.counters[session_id]['is_paused']:
            del socketio.status_threads[session_id]

def read_port_from_file():
    port = DEFAULT_PORT
    try:
        with open(PORT_FILE, 'r', encoding='utf-8') as f:
            content = f.read().strip()
            if content:
                try:
                    port = int(content)
                    if not (1024 <= port <= 65535):
                        logger.warning(f"Port {port} from {PORT_FILE} is outside typical range (1024-65535). Using default {DEFAULT_PORT}.")
                        port = DEFAULT_PORT
                except ValueError:
                    logger.warning(f"Invalid port number '{content}' in {PORT_FILE}. Using default {DEFAULT_PORT}.")
            else:
                logger.warning(f"{PORT_FILE} is empty. Using default {DEFAULT_PORT}.")
    except FileNotFoundError:
        logger.warning(f"{PORT_FILE} not found. Using default {DEFAULT_PORT}.")
    except Exception as e:
        logger.error(f"Error reading {PORT_FILE}: {e}. Using default {DEFAULT_PORT}.")
    return port

if __name__ == '__main__':
    for item in os.listdir('.'):
        if item.startswith('session_') and os.path.isdir(item):
            session_id_from_dir = item.replace('session_', '')
            session_data_path = os.path.join(item, SESSION_DATA_FILE)
            if not os.path.exists(session_data_path):
                logger.warning(f"Removing incomplete session directory: {item}")
                try:
                    shutil.rmtree(item)
                except Exception as e:
                    logger.error(f"Error removing old session directory {item}: {e}")
            else:
                try:
                    with open(session_data_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        if data.get('is_running', False) and not data.get('completed', False):
                            data['is_running'] = True
                            data['is_paused'] = True
                            active_sessions[session_id_from_dir] = data
                            save_session_data(session_id_from_dir)
                            logger.info(f"Loaded and auto-paused session {session_id_from_dir} from disk.")

                            proxy_file = data.get('proxy_file')
                            if proxy_file and os.path.exists(proxy_file):
                                try:
                                    with open(proxy_file, 'r', encoding='utf-8', errors='ignore') as f_proxy:
                                        loaded_proxy_lines = f_proxy.read().strip().splitlines()
                                        # Update chk.counters directly with the loaded proxy lines
                                        if session_id_from_dir not in chk.counters:
                                            chk.counters[session_id_from_dir] = {}
                                        chk.counters[session_id_from_dir]['proxy_lines'] = loaded_proxy_lines
                                        logger.info(f"Re-read proxy file for auto-paused session {session_id_from_dir} on startup.")
                                except Exception as e:
                                    logger.error(f"Error re-reading proxy file for auto-paused session {session_id_from_dir} on startup: {e}")

                            if session_id_from_dir not in chk.counters:
                                chk.counters[session_id_from_dir] = {} # Initialize if not present
                            
                            # Update chk.counters with loaded data, preserving proxy_lines if already set
                            chk.counters[session_id_from_dir].update({
                                'checked': data.get('checked_on_stop', 0),
                                'invalid': 0, 'hits': 0, 'custom': 0,
                                'total_mega_fan': 0, 'total_fan_member': 0, 'total_ultimate_mega': 0,
                                'errors': 0, 'retries': 0,
                                'is_running': False, 'is_paused': False,
                                'completed': False,
                                'start_time': datetime.fromisoformat(data['timestamp']),
                                'end_time': None,
                                'total_lines': data['combo_line_count'],
                                'last_pause_time': datetime.now(),
                                'total_paused_time': timedelta(0),
                                'threads': data.get('threads', 10),
                                'proxy_type': data.get('proxy_type', 'http'),
                                '_restart_signal': False # Ensure no restart signal on startup
                            })
                            logger.info(f"Re-initialized chk.counters for auto-paused session {session_id_from_dir} on startup.")
                            if not hasattr(socketio, 'status_threads'):
                                socketio.status_threads = {}
                            if session_id_from_dir not in socketio.status_threads or not socketio.status_threads[session_id_from_dir].is_alive():
                                status_thread = threading.Thread(
                                    target=update_status_websocket,
                                    args=(session_id_from_dir,)
                                )
                                status_thread.daemon = True
                                status_thread.start()
                                socketio.status_threads[session_id_from_dir] = status_thread
                            else:
                                logger.info(f"Status update thread for {session_id_from_dir} already running on startup.")

                        else:
                            active_sessions[session_id_from_dir] = data
                            logger.info(f"Loaded non-running/completed session {session_id_from_dir} from disk.")
                except json.JSONDecodeError as e:
                    logger.error(f"Corrupt session_data.json in {item}: {e}. Consider manual cleanup.")
                except Exception as e:
                    logger.error(f"Error loading session {session_id_from_dir} on startup: {e}")

    app_port = read_port_from_file()
    logger.info(f"Starting Flask app on port {app_port}...")

    # Custom startup messages to show /crun route
    import socket
    hostname = socket.gethostname()
    try:
        local_ip = socket.gethostbyname(hostname)
        logger.info(Fore.GREEN + f"* Running on http://127.0.0.1:{app_port}/crun" + Fore.RESET)
        logger.info(Fore.GREEN + f"* Running on http://{local_ip}:{app_port}/crun" + Fore.RESET)
    except Exception as e:
        logger.warning(f"Could not determine local IP: {e}")
        logger.info(Fore.GREEN + f"* Running on http://127.0.0.1:{app_port}/crun" + Fore.RESET)

    socketio.run(app, host='0.0.0.0', port=app_port, debug=True, allow_unsafe_werkzeug=True)



def reset_for_new_inputs(session_id, session_data):
    """Clear stats and truncate hits/custom when starting after new inputs."""
    chk.counters.setdefault(session_id, {})
    c = chk.counters[session_id]
    for k in ['checked','invalid','hits','custom','total_mega_fan','total_fan_member','total_ultimate_mega','errors','retries']:
        c[k] = 0
    c['completed'] = False
    c['start_time'] = datetime.now()
    c['end_time'] = None
    c['total_paused_time'] = timedelta(0)
    c['last_pause_time'] = None

    session_dir = ensure_session_directory(session_id)
    for fn in ('hits.txt','custom.txt'):
        try:
            open(os.path.join(session_dir, fn), 'w', encoding='utf-8').close()
        except Exception as e:
            logger.error(f"reset_for_new_inputs truncate {fn} failed for {session_id}: {e}")

    socketio.emit('stats_update', chk.generate_stats_text(session_id), room=session_id)


@socketio.on('dedupe_hits')
def handle_dedupe_hits(data):
    session_id = data.get('session_id')
    before, after = dedupe_hits_file(session_id)
    emit('status_message', {'type': 'success', 'message': f'✅ Deduped hits: {before} → {after}'}, room=session_id)
