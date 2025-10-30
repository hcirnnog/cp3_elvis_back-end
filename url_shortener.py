from flask import Flask, request, redirect, jsonify, render_template, send_from_directory
from datetime import datetime
from contextlib import contextmanager
import pymysql
from pymongo import MongoClient
import requests

app = Flask(__name__)

MYSQL_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'fiap',
    'database': 'url_shortener'
}

mongo_client = MongoClient('mongodb://localhost:27017/')
mongo_db = mongo_client['url_shortener']
access_logs = mongo_db['access_logs']
creation_logs = mongo_db['creation_logs']  # Nova coleção para histórico de criações

@contextmanager
def get_db():
    conn = pymysql.connect(**MYSQL_CONFIG)
    try:
        yield conn
    finally:
        conn.close()

def init_db():
    try:
        temp_config = MYSQL_CONFIG.copy()
        temp_config.pop('database', None)
        conn = pymysql.connect(**temp_config)
        cursor = conn.cursor()
        cursor.execute("CREATE DATABASE IF NOT EXISTS url_shortener")
        cursor.close()
        conn.close()
        
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS urls (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    short_code VARCHAR(50) UNIQUE NOT NULL,
                    destination_url TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    access_count INT DEFAULT 0,
                    INDEX idx_short_code (short_code)
                )
            """)
            conn.commit()
            cursor.close()
        print("Banco de dados inicializado com sucesso.")
    except Exception as e:
        print(f"Erro ao inicializar banco de dados: {e}")
        raise

def validate_url(url):
    try:
        response = requests.head(url, allow_redirects=True, timeout=5)  # Permite redirecionamentos
        if response.status_code in [200, 201, 202, 301, 302, 303, 307, 308]:  # Permite redirecionamentos
            return True, None
        return False, f"URL inválida (Status: {response.status_code})"
    except:
        return False, "Erro ao validar URL"

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/styles.css')
def serve_css():
    return send_from_directory('.', 'styles.css')

@app.route('/api/health')
def health():
    return jsonify({'status': 'ok', 'timestamp': datetime.utcnow().isoformat()})

@app.route('/api/urls', methods=['GET'])
def list_urls():
    try:
        with get_db() as conn:
            cursor = conn.cursor(pymysql.cursors.DictCursor)
            cursor.execute("SELECT id, short_code, destination_url, created_at, access_count FROM urls ORDER BY created_at DESC")
            urls = cursor.fetchall()
            cursor.close()
            for url in urls:
                if url['created_at']:
                    url['created_at'] = url['created_at'].isoformat()
            return jsonify({'success': True, 'data': urls, 'count': len(urls)})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/urls', methods=['POST'])
def create_url():
    try:
        data = request.get_json()
        short_code = data.get('short_code', '').strip()
        destination_url = data.get('destination_url', '').strip()
        if not short_code or not destination_url:
            return jsonify({'success': False, 'error': 'Campos obrigatórios ausentes'}), 400
        if not short_code.replace('-', '').replace('_', '').isalnum():
            return jsonify({'success': False, 'error': 'Código inválido'}), 400
        is_valid, error = validate_url(destination_url)
        if not is_valid:
            return jsonify({'success': False, 'error': error}), 400
        with get_db() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("INSERT INTO urls (short_code, destination_url) VALUES (%s, %s)", (short_code, destination_url))
                conn.commit()
                url_id = cursor.lastrowid
                cursor.close()
                # Salva log de criação no MongoDB
                creation_logs.insert_one({
                    'short_code': short_code,
                    'destination_url': destination_url,
                    'client_ip': request.headers.get('X-Forwarded-For', request.remote_addr),
                    'user_agent': request.headers.get('User-Agent', 'Unknown'),
                    'created_at': datetime.utcnow().isoformat()
                })
                return jsonify({
                    'success': True,
                    'data': {
                        'id': url_id,
                        'short_code': short_code,
                        'destination_url': destination_url,
                        'short_url': f'/{short_code}'
                    }
                }), 201
            except pymysql.IntegrityError:
                cursor.close()
                return jsonify({'success': False, 'error': 'Código já existe'}), 409
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/creation-history')
def get_creation_history():
    try:
        logs = list(creation_logs.find({}, {'_id': 0}).sort('created_at', -1))
        return jsonify({
            'success': True,
            'data': logs,
            'count': len(logs)
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/urls/<short_code>', methods=['DELETE'])
def delete_url(short_code):
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM urls WHERE short_code = %s", (short_code,))
            deleted = cursor.rowcount > 0
            conn.commit()
            cursor.close()
            if deleted:
                access_logs.delete_many({'short_code': short_code})
                creation_logs.delete_many({'short_code': short_code})  # Remove do histórico
                return jsonify({'success': True})
            return jsonify({'success': False, 'error': 'URL não encontrada'}), 404
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/urls/<short_code>/stats')
def get_stats(short_code):
    try:
        with get_db() as conn:
            cursor = conn.cursor(pymysql.cursors.DictCursor)
            cursor.execute("SELECT short_code, destination_url, access_count, created_at FROM urls WHERE short_code = %s", (short_code,))
            url_data = cursor.fetchone()
            cursor.close()
            if not url_data:
                return jsonify({'success': False, 'error': 'URL não encontrada'}), 404
            if url_data['created_at']:
                url_data['created_at'] = url_data['created_at'].isoformat()
            return jsonify({'success': True, 'data': url_data})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/urls/<short_code>/history')
def get_history(short_code):
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT id FROM urls WHERE short_code = %s", (short_code,))
            if not cursor.fetchone():
                cursor.close()
                return jsonify({'success': False, 'error': 'URL não encontrada'}), 404
            cursor.close()
        logs = list(access_logs.find({'short_code': short_code}, {'_id': 0}).sort('accessed_at', -1))
        return jsonify({
            'success': True,
            'data': {
                'short_code': short_code,
                'total_accesses': len(logs),
                'history': logs
            }
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/<short_code>')
def redirect_url(short_code):
    try:
        with get_db() as conn:
            cursor = conn.cursor(pymysql.cursors.DictCursor)
            cursor.execute("SELECT destination_url FROM urls WHERE short_code = %s", (short_code,))
            result = cursor.fetchone()
            if not result:
                cursor.close()
                return jsonify({
                    'success': False,
                    'error': 'URL não encontrada',
                    'short_code': short_code
                }), 404
            cursor.execute("UPDATE urls SET access_count = access_count + 1 WHERE short_code = %s", (short_code,))
            conn.commit()
            cursor.close()
            access_logs.insert_one({
                'short_code': short_code,
                'client_ip': request.headers.get('X-Forwarded-For', request.remote_addr),
                'user_agent': request.headers.get('User-Agent', 'Unknown'),
                'accessed_at': datetime.utcnow().isoformat()
            })
            return redirect(result['destination_url'], code=302)
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

if __name__ == '__main__':
    init_db()
    print("\n" + "="*50)
    print("Encurtador de URLs - Backend")
    print("="*50)
    print("\nAcesse: http://localhost:5000")
    print("\nEndpoints API:")
    print("  GET    /api/health")
    print("  GET    /api/urls")
    print("  POST   /api/urls")
    print("  GET    /api/creation-history")
    print("  DELETE /api/urls/<code>")
    print("  GET    /api/urls/<code>/stats")
    print("  GET    /api/urls/<code>/history")
    print("  GET    /<code>")
    print("="*50 + "\n")
    app.run(debug=True, port=5000)
