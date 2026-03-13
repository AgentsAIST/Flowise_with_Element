import os
import signal
import threading
import subprocess
import hashlib
import json
import asyncio
from datetime import datetime
from flask import Flask, render_template, request, jsonify, redirect, url_for, session
import psycopg2
from psycopg2.extras import RealDictCursor
import bcrypt
import docker
import sys
import logging

from dotenv import load_dotenv

load_dotenv()

SYNAPSE_SERVER_NAME = os.getenv('SYNAPSE_SERVER_NAME', 'localhost')
SYNAPSE_PUBLIC_URL = os.getenv('SYNAPSE_PUBLIC_URL', 'http://localhost:8008')
SYNAPSE_INTERNAL_URL = os.getenv('SYNAPSE_INTERNAL_URL', 'http://synapse:8008')
ORCHESTRATOR_PUBLIC_URL = os.getenv('ORCHESTRATOR_PUBLIC_URL', 'http://localhost:8001')

LOGIN_TEMPLATE = 'login.html'
INDEX_TEMPLATE = 'index.html'

running_bots = {}

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.secret_key = os.getenv('ORCHESTRATOR_WEB_CLIENT_SECRET', 'default_secret_1111111')

import time

def wait_for_db(max_retries=30, delay=2):
    for attempt in range(max_retries):
        try:
            conn = get_db_connection()
            conn.close()
            logger.info("✅ Database connection successful")
            return True
        except psycopg2.OperationalError as e:
            logger.warning(f"⚠️ Database not ready (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(delay)
    return False

try:
    docker_client = docker.from_env()
    logger.info("Docker client initialized successfully")
except Exception as e:
    logger.error(f"Failed to initialize Docker client: {e}")
    docker_client = None

def get_db_connection():
    conn = psycopg2.connect(
        host=os.getenv('DB_HOST', 'postgres'),
        database=os.getenv('DB_NAME', 'orchestrator'),
        user=os.getenv('DB_USER', 'orchestrator_user'),
        password=os.getenv('DB_PASSWORD', 'orchestrator_pass')
    )
    return conn

def init_db():
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            username VARCHAR(255) UNIQUE NOT NULL,
            password_hash VARCHAR(255) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS bots (
            id SERIAL PRIMARY KEY,
            bot_user_id VARCHAR(255) UNIQUE NOT NULL,
            flowise_url TEXT NOT NULL,
            password_hash VARCHAR(255) NOT NULL,
            status VARCHAR(20) DEFAULT 'stopped',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS bot_processes (
            id SERIAL PRIMARY KEY,
            bot_id INTEGER UNIQUE REFERENCES bots(id) ON DELETE CASCADE,
            process_id INTEGER,
            started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_bot_processes_bot_id 
        ON bot_processes(bot_id)
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sessions (
            id SERIAL PRIMARY KEY,
            bot_id INTEGER NOT NULL REFERENCES bots(id) ON DELETE CASCADE,
            user_id VARCHAR(255) NOT NULL,
            room_id VARCHAR(255) NOT NULL,
            session_id VARCHAR(255),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(bot_id, user_id, room_id)
        )
    """)
    
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_sessions_bot_id 
        ON sessions(bot_id)
    """)
    
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_sessions_user_room 
        ON sessions(user_id, room_id)
    """)
    
    conn.commit()
    cursor.close()
    conn.close()
    logger.info("Database initialized successfully")

@app.route('/')
def index():
    if not session.get('authenticated'):
        return redirect(url_for('login'))
    
    return render_template(INDEX_TEMPLATE, 
        synapse_server_name=SYNAPSE_SERVER_NAME,
        synapse_public_url=SYNAPSE_PUBLIC_URL,
        orchestrator_public_url=ORCHESTRATOR_PUBLIC_URL
    )

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        password = request.form['password']
        admin_password = os.getenv('ORCHESTRATOR_WEB_CLIENT_SECRET', '1111111')
        
        if password == admin_password:
            session['authenticated'] = True
            logger.info("User logged in successfully")
            return redirect(url_for('index'))
        else:
            logger.warning("Failed login attempt")
            return render_template(LOGIN_TEMPLATE, error="Invalid password")
    
    return render_template(LOGIN_TEMPLATE, error=None)

@app.route('/logout')
def logout():
    session.pop('authenticated', None)
    logger.info("User logged out")
    return redirect(url_for('login'))

def register_matrix_user_simple(username, password, is_admin=False):
    try:
        logger.info(f"Registering user: {username} for domain {SYNAPSE_SERVER_NAME}")

        if not username.startswith('@'):
            username = '@' + username
        if not username.endswith(f':{SYNAPSE_SERVER_NAME}'):
            if ':' in username:
                username = username.split(':')[0]
            username = username + f':{SYNAPSE_SERVER_NAME}'
        
        logger.info(f"Formatted username: {username}")

        return register_via_docker_container(username, password, is_admin)
            
    except Exception as e:
        logger.error(f"Error in register_matrix_user_simple: {e}", exc_info=True)
        return False, f"Unexpected error: {str(e)}"

def register_via_docker_container(username, password, is_admin=False):
    try:
        if docker_client is None:
            return False, "Docker client not available"

        container = docker_client.containers.get('synapse')

        localpart = username
        if localpart.startswith('@'):
            localpart = localpart[1:]
        if ':' in localpart:
            localpart = localpart.split(':')[0]
        
        logger.info(f"Registering localpart: {localpart} on server {SYNAPSE_SERVER_NAME}")

        cmd = [
            'docker', 'exec', '-i', 'synapse',
            'register_new_matrix_user',
            SYNAPSE_INTERNAL_URL,
            '-c', '/data/homeserver.yaml',
            '--user', localpart,
            '--password', password,
        ]
        
        if is_admin:
            cmd.append('--admin')
        else:
            cmd.append('--no-admin')
        
        logger.info(f"Executing command: {' '.join(cmd)}")
        
        process = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        stdout, stderr = process.communicate()
        
        logger.info(f"STDOUT: {stdout}")
        logger.info(f"STDERR: {stderr}")
        logger.info(f"Return code: {process.returncode}")
        
        if process.returncode == 0 or "already exists" in stderr.lower() or "already exists" in stdout.lower():
            return True, "User created successfully or already exists"
        else:
            return False, f"Registration failed: {stdout} {stderr}"
            
    except Exception as e:
        logger.error(f"Docker registration error: {e}", exc_info=True)
        return False, f"Container registration error: {str(e)}"

@app.route('/api/create_user', methods=['POST'])
def create_user():
    if not session.get('authenticated'):
        logger.warning("Unauthorized access attempt to create_user")
        return jsonify({'error': 'Not authenticated'}), 401
    
    username = request.form['username']
    password = request.form['password']
    is_admin = request.form.get('is_admin') == 'on'
    
    logger.info(f"Creating user: {username}, admin: {is_admin}")

    if not username:
        return jsonify({'error': 'Username is required'}), 400

    if not username.startswith('@'):
        username = '@' + username
    if not username.endswith(':localhost'):
        username = username + ':localhost'

    if len(password) < 3:
        return jsonify({'error': 'Password must be at least 3 characters'}), 400

    try:
        password_hash = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
    except Exception as e:
        logger.error(f"Error hashing password: {e}")
        return jsonify({'error': 'Error processing password'}), 500
    
    try:
        success, message = register_matrix_user_simple(username, password, is_admin)
        
        if not success:
            if "already exists" not in message.lower():
                logger.error(f"Failed to create user in Synapse: {message}")
                return jsonify({'error': f'Failed to create user: {message}'}), 500
            else:
                logger.info(f"User already exists in Synapse: {message}")

        conn = get_db_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                "INSERT INTO users (username, password_hash) VALUES (%s, %s)",
                (username, password_hash)
            )
            conn.commit()
            logger.info(f"User {username} saved to database")
        except psycopg2.IntegrityError as e:
            conn.rollback()
            logger.info(f"User {username} already exists in database: {e}")
        except Exception as e:
            conn.rollback()
            logger.error(f"Error saving user to database: {e}")
        
        cursor.close()
        conn.close()
        
        logger.info(f"User {username} created successfully")
        return jsonify({'success': True, 'message': f'User {username} created successfully'})
    
    except Exception as e:
        logger.error(f"Error in create_user: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500

@app.route('/api/bots', methods=['GET', 'POST'])
def manage_bots():
    if not session.get('authenticated'):
        return jsonify({'error': 'Not authenticated'}), 401
    
    if request.method == 'GET':
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("SELECT * FROM bots ORDER BY created_at DESC")
        bots = cursor.fetchall()
        cursor.close()
        conn.close()
        return jsonify(bots)
    
    elif request.method == 'POST':
        bot_user_id = request.form['bot_user_id']
        flowise_url = request.form['flowise_url']
        password = request.form['bot_password']
        
        logger.info(f"Creating bot: {bot_user_id}")

        if not bot_user_id:
            return jsonify({'error': 'Bot user ID is required'}), 400
        
        if not bot_user_id.startswith('@'):
            bot_user_id = '@' + bot_user_id
        if not bot_user_id.endswith(':localhost'):
            bot_user_id = bot_user_id + ':localhost'

        try:
            password_hash = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
        except Exception as e:
            logger.error(f"Error hashing bot password: {e}")
            return jsonify({'error': 'Error processing password'}), 500
        
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO bots (bot_user_id, flowise_url, password_hash) VALUES (%s, %s, %s)",
                (bot_user_id, flowise_url, password_hash)
            )
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info(f"Bot {bot_user_id} created successfully")
            return jsonify({'success': True, 'message': 'Bot created successfully'})
        except Exception as e:
            logger.error(f"Error creating bot in database: {e}", exc_info=True)
            return jsonify({'error': str(e)}), 500

@app.route('/api/bot/<int:bot_id>/action', methods=['POST'])
def bot_action(bot_id):
    try:
        if not session.get('authenticated'):
            return jsonify({'error': 'Not authenticated'}), 401
        
        action = request.form.get('action')
        provided_password = request.form.get('password')
        
        if not action or not provided_password:
            return jsonify({'error': 'Action and password are required'}), 400
        
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        cursor.execute("SELECT * FROM bots WHERE id = %s", (bot_id,))
        bot = cursor.fetchone()
        
        if not bot:
            cursor.close()
            conn.close()
            return jsonify({'error': 'Bot not found'}), 404

        admin_password = os.getenv('ORCHESTRATOR_ADMIN_PASSWORD', '1111111')
        
        try:
            is_valid_bot_password = bcrypt.checkpw(
                provided_password.encode('utf-8'), 
                bot['password_hash'].encode('utf-8')
            )
        except Exception as e:
            logger.error(f"Password check error: {e}")
            is_valid_bot_password = False

        is_valid_admin_password = (provided_password == admin_password)
        
        if not is_valid_bot_password and not is_valid_admin_password:
            cursor.close()
            conn.close()
            return jsonify({'error': 'Invalid password'}), 401
        bot_password_to_use = provided_password if is_valid_bot_password else admin_password
        
        try:
            if action == 'start':
                start_bot_process(
                    bot_id, 
                    bot['bot_user_id'], 
                    bot['flowise_url'], 
                    bot_password_to_use
                )
                cursor.execute("UPDATE bots SET status = 'running' WHERE id = %s", (bot_id,))
                conn.commit()
                result = {'success': True, 'message': 'Bot started successfully'}
                status_code = 200
                
            elif action == 'stop':
                stop_bot_process(bot_id)
                cursor.execute("UPDATE bots SET status = 'stopped' WHERE id = %s", (bot_id,))
                conn.commit()
                result = {'success': True, 'message': 'Bot stopped successfully'}
                status_code = 200
                
            elif action == 'delete':
                try:
                    stop_bot_process(bot_id)
                except Exception as e:
                    logger.warning(f"Non-critical error stopping bot {bot_id} before deletion: {e}")

                cursor.execute("DELETE FROM bot_processes WHERE bot_id = %s", (bot_id,))
                cursor.execute("DELETE FROM bots WHERE id = %s", (bot_id,))
                conn.commit()
                result = {'success': True, 'message': 'Bot deleted successfully'}
                status_code = 200
            else:
                result = {'error': 'Invalid action'}
                status_code = 400
                
        except Exception as e:
            logger.error(f"Error performing bot action {action}: {e}", exc_info=True)
            conn.rollback()
            result = {'error': f'Failed to {action} bot: {str(e)}'}
            status_code = 500
            
    except Exception as e:
        logger.error(f"Unexpected error in bot_action: {e}", exc_info=True)
        result = {'error': f'Internal server error: {str(e)}'}
        status_code = 500
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()
    
    return jsonify(result), status_code

def start_cleanup_scheduler():
    def cleanup_loop():
        while True:
            try:
                cleaned, updated = cleanup_dead_processes()
                if cleaned > 0 or updated > 0:
                    logger.info(f"✅ Background cleanup completed: {cleaned} dead processes, {updated} status updates")
            except Exception as e:
                logger.error(f"Cleanup scheduler error: {e}")

            threading.Event().wait(300)
    
    cleanup_thread = threading.Thread(target=cleanup_loop, daemon=True)
    cleanup_thread.start()
    logger.info("🧹 Background cleanup scheduler started")

def cleanup_dead_processes():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT bot_id, process_id FROM bot_processes")
        processes = cursor.fetchall()
        
        cleaned_count = 0
        for bot_id, pid in processes:
            try:
                os.kill(pid, 0)
            except OSError:
                cursor.execute(
                    "DELETE FROM bot_processes WHERE bot_id = %s AND process_id = %s",
                    (bot_id, pid)
                )
                cleaned_count += 1
                logger.info(f"🧹 Cleaned up dead process record for bot {bot_id}, PID {pid}")
        
        if cleaned_count > 0:
            conn.commit()
            logger.info(f"🧹 Cleaned up {cleaned_count} dead process records")

        cursor.execute("""
            UPDATE bots 
            SET status = 'stopped' 
            WHERE status = 'running' 
            AND id NOT IN (SELECT bot_id FROM bot_processes)
        """)
        updated_count = cursor.rowcount
        if updated_count > 0:
            conn.commit()
            logger.info(f"🔄 Updated status to 'stopped' for {updated_count} bots without processes")
        
        cursor.close()
        conn.close()
        
        return cleaned_count, updated_count
        
    except Exception as e:
        logger.error(f"Error cleaning up dead processes: {e}")
        return 0, 0

def start_bot_process(bot_id, bot_user_id, flowise_url, password):
    try:
        logger.info(f"Starting bot {bot_id} ({bot_user_id}) with Flowise URL: {flowise_url}")

        bot_script_path = "/app/matrix-bot.py"
        if not os.path.exists(bot_script_path):
            error_msg = f"Bot script not found at {bot_script_path}"
            logger.error(error_msg)
            raise FileNotFoundError(error_msg)

        server_name = os.getenv('SYNAPSE_SERVER_NAME', 'matrix.local')

        if not bot_user_id.startswith('@'):
            bot_user_id = '@' + bot_user_id
        if not bot_user_id.endswith(f':{server_name}'):
            if ':' in bot_user_id:
                bot_user_id = bot_user_id.split(':')[0]
            bot_user_id = bot_user_id + f':{server_name}'
        
        logger.info(f"✅ Using formatted bot user ID: {bot_user_id}")

        env = os.environ.copy()
        env.update({
            'BOT_HOMESERVER': os.getenv('SYNAPSE_INTERNAL_URL', 'http://synapse:8008'),
            'BOT_USER_ID': bot_user_id,
            'BOT_PASSWORD': password,
            'BOT_FLOWISE_URL': flowise_url,
            'BOT_ID': str(bot_id),
            'SERVER_NAME': server_name
        })

        log_dir = "/app/bot_logs"
        os.makedirs(log_dir, exist_ok=True)

        log_file = f"{log_dir}/bot_{bot_id}.log"

        with open(log_file, 'a') as log_f:
            process = subprocess.Popen(
                [
                    sys.executable, 
                    bot_script_path,
                    '--homeserver', env['BOT_HOMESERVER'],
                    '--user', bot_user_id,
                    '--password', password,
                    '--flowise-url', flowise_url,
                    '--bot-id', str(bot_id)
                ],
                stdout=log_f,
                stderr=subprocess.STDOUT,
                text=True,
                env=env,
                preexec_fn=os.setsid
            )

        running_bots[bot_id] = {
            'process': process,
            'bot_user_id': bot_user_id,
            'log_file': log_file,
            'started_at': datetime.now()
        }

        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(
            "DELETE FROM bot_processes WHERE bot_id = %s",
            (bot_id,)
        )

        cursor.execute(
            "INSERT INTO bot_processes (bot_id, process_id) VALUES (%s, %s)",
            (bot_id, process.pid)
        )
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info(f"✅ Bot {bot_id} started with PID: {process.pid}, Log: {log_file}")

        monitor_thread = threading.Thread(
            target=monitor_bot_process,
            args=(bot_id, process, log_file),
            daemon=True
        )
        monitor_thread.start()
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Error starting bot {bot_id}: {e}", exc_info=True)
        if bot_id in running_bots:
            try:
                process = running_bots[bot_id]['process']
                os.killpg(os.getpgid(process.pid), signal.SIGTERM)
                del running_bots[bot_id]
            except:
                pass
        raise

def stop_bot_process(bot_id):
    try:
        logger.info(f"Stopping bot {bot_id}")

        if bot_id in running_bots:
            process_info = running_bots[bot_id]
            process = process_info['process']
            
            try:
                if process.poll() is None:
                    logger.info(f"Terminating bot {bot_id} process group (PID: {process.pid})")
                    os.killpg(os.getpgid(process.pid), signal.SIGTERM)
                    try:
                        process.wait(timeout=10)
                        logger.info(f"✅ Bot {bot_id} terminated gracefully")
                    except subprocess.TimeoutExpired:
                        logger.warning(f"⚠️ Bot {bot_id} didn't terminate gracefully, forcing kill")
                        os.killpg(os.getpgid(process.pid), signal.SIGKILL)
                else:
                    logger.info(f"Bot {bot_id} process already terminated (exit code: {process.returncode})")
            except ProcessLookupError:
                logger.warning(f"Bot {bot_id} process already terminated")
            except Exception as e:
                logger.error(f"Error terminating bot {bot_id} process: {e}")

            del running_bots[bot_id]

        conn = get_db_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute(
                "DELETE FROM bot_processes WHERE bot_id = %s",
                (bot_id,)
            )

            cursor.execute(
                "UPDATE bots SET status = 'stopped' WHERE id = %s AND status != 'stopped'",
                (bot_id,)
            )
            
            conn.commit()
            logger.info(f"🧹 Cleaned up database records for bot {bot_id}")
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Error cleaning up database for bot {bot_id}: {e}")
            raise
        finally:
            cursor.close()
            conn.close()
            
        return True
        
    except Exception as e:
        logger.error(f"❌ Critical error stopping bot {bot_id}: {e}", exc_info=True)
        raise

def monitor_bot_process(bot_id, process, log_file):
    try:
        process.wait()
        exit_code = process.returncode
        logger.info(f"Bot {bot_id} process terminated with code: {exit_code}")

        with open(log_file, 'a') as f:
            f.write(f"\n[{datetime.now()}] Bot process terminated with exit code: {exit_code}\n")

        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE bots SET status = 'stopped' WHERE id = %s",
            (bot_id,)
        )
        conn.commit()
        cursor.close()
        conn.close()

        if bot_id in running_bots:
            del running_bots[bot_id]
            
    except Exception as e:
        logger.error(f"Error monitoring bot {bot_id}: {e}")

@app.route('/api/bot/<int:bot_id>/logs')
def get_bot_logs(bot_id):
    if not session.get('authenticated'):
        return jsonify({'error': 'Not authenticated'}), 401
    
    log_file = f"/app/bot_logs/bot_{bot_id}.log"
    try:
        if os.path.exists(log_file):
            with open(log_file, 'r') as f:
                logs = f.read()
            return jsonify({'success': True, 'logs': logs})
        else:
            return jsonify({'success': False, 'error': 'Log file not found'})
    except Exception as e:
        logger.error(f"Error reading logs for bot {bot_id}: {e}")
        return jsonify({'success': False, 'error': str(e)})

if __name__ == '__main__':

    if not wait_for_db():
        logger.error("❌ Failed to connect to database after multiple attempts. Exiting.")
        sys.exit(1)

    init_db()
    
    start_cleanup_scheduler()

    host = os.getenv('ORCHESTRATOR_HOST', '0.0.0.0')
    port = int(os.getenv('ORCHESTRATOR_PORT', 8001))

    logger.info(f"🚀 Starting orchestrator on {host}:{port}")
    logger.info(f"📡 Public URL: {ORCHESTRATOR_PUBLIC_URL}")

    app.run(host=host, port=port, debug=True)