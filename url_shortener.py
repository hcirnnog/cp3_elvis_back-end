from flask import Flask, request, redirect, jsonify
from datetime import datetime
import mysql.connector
from pymongo import MongoClient
import requests
from urllib.parse import urlparse

app = Flask(__name__)

# Configurações do MySQL (URLs e redirecionamentos)
MYSQL_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'fiap',
    'database': 'url_shortener'
}

# Configurações do MongoDB (logs de acesso)
MONGO_URI = 'mongodb://localhost:27017/'
mongo_client = MongoClient(MONGO_URI)
mongo_db = mongo_client['url_shortener']
access_logs = mongo_db['access_logs']

# Funções auxiliares para MySQL
def get_mysql_connection():
    return mysql.connector.connect(**MYSQL_CONFIG)

def get_mysql_connection_no_db():
    config = MYSQL_CONFIG.copy()
    del config['database']
    return mysql.connector.connect(**config)

def init_mysql_db():
    """Inicializa o banco de dados MySQL e cria a tabela se não existir"""
    # Primeiro, conecta sem especificar o banco de dados para criar o banco se necessário
    conn = get_mysql_connection_no_db()
    cursor = conn.cursor()
    
    cursor.execute("CREATE DATABASE IF NOT EXISTS url_shortener")
    
    conn.commit()
    cursor.close()
    conn.close()
    
    # Agora conecta ao banco de dados e cria a tabela
    conn = get_mysql_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS urls (
            id INT AUTO_INCREMENT PRIMARY KEY,
            short_code VARCHAR(50) UNIQUE NOT NULL,
            destination_url TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            access_count INT DEFAULT 0
        )
    """)
    
    conn.commit()
    cursor.close()
    conn.close()

def validate_destination_url(url):
    """
    Valida se a URL de destino não é um redirecionamento.
    Retorna (is_valid, status_code, final_url)
    """
    try:
        response = requests.head(url, allow_redirects=False, timeout=5)
        
        # Status codes de redirecionamento: 301, 302, 303, 307, 308
        if response.status_code in [301, 302, 303, 307, 308]:
            return False, response.status_code, response.headers.get('Location', 'Unknown')
        
        return True, response.status_code, url
    except requests.RequestException as e:
        # Se houver erro na requisição, aceita a URL mas registra o problema
        return True, None, str(e)

# Rotas da API

# RF1: Listar todos os redirecionamentos
@app.route('/api/urls', methods=['GET'])
def list_urls():
    """Lista todos os redirecionamentos cadastrados"""
    try:
        conn = get_mysql_connection()
        cursor = conn.cursor(dictionary=True)
        
        cursor.execute("""
            SELECT id, short_code, destination_url, created_at, access_count 
            FROM urls 
            ORDER BY created_at DESC
        """)
        
        urls = cursor.fetchall()
        
        # Converte datetime para string
        for url in urls:
            if url['created_at']:
                url['created_at'] = url['created_at'].isoformat()
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'data': urls,
            'count': len(urls)
        }), 200
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

# RF2 e RF3: Cadastrar nova URL curta
@app.route('/api/urls', methods=['POST'])
def create_short_url():
    """Cadastra uma nova URL curta"""
    try:
        data = request.get_json()
        
        if not data or 'short_code' not in data or 'destination_url' not in data:
            return jsonify({
                'success': False,
                'error': 'Campos obrigatórios: short_code e destination_url'
            }), 400
        
        short_code = data['short_code'].strip()
        destination_url = data['destination_url'].strip()
        
        # Validações básicas
        if not short_code or not destination_url:
            return jsonify({
                'success': False,
                'error': 'Os campos não podem estar vazios'
            }), 400
        
        # Valida se o short_code contém apenas caracteres alfanuméricos
        if not short_code.replace('-', '').replace('_', '').isalnum():
            return jsonify({
                'success': False,
                'error': 'O código curto deve conter apenas letras, números, hífens e underscores'
            }), 400
        
        # RF3: Valida se a URL de destino não é um redirecionamento
        is_valid, status_code, info = validate_destination_url(destination_url)
        
        if not is_valid:
            return jsonify({
                'success': False,
                'error': f'A URL de destino é um redirecionamento (HTTP {status_code})',
                'message': 'Por questões de segurança, não é permitido cadastrar URLs que redirecionam para outras páginas.',
                'redirect_to': info
            }), 400
        
        # Insere no banco de dados
        conn = get_mysql_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                INSERT INTO urls (short_code, destination_url) 
                VALUES (%s, %s)
            """, (short_code, destination_url))
            
            conn.commit()
            url_id = cursor.lastrowid
            
            cursor.close()
            conn.close()
            
            return jsonify({
                'success': True,
                'data': {
                    'id': url_id,
                    'short_code': short_code,
                    'destination_url': destination_url,
                    'short_url': f'/{short_code}'
                },
                'message': 'URL curta criada com sucesso'
            }), 201
            
        except mysql.connector.IntegrityError:
            cursor.close()
            conn.close()
            return jsonify({
                'success': False,
                'error': 'Este código curto já está em uso'
            }), 409
            
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

# RF4: Excluir um redirecionamento
@app.route('/api/urls/<short_code>', methods=['DELETE'])
def delete_url(short_code):
    """Exclui um redirecionamento"""
    try:
        conn = get_mysql_connection()
        cursor = conn.cursor()
        
        cursor.execute("DELETE FROM urls WHERE short_code = %s", (short_code,))
        
        if cursor.rowcount == 0:
            cursor.close()
            conn.close()
            return jsonify({
                'success': False,
                'error': 'URL curta não encontrada'
            }), 404
        
        conn.commit()
        
        # Remove também os logs de acesso do MongoDB
        access_logs.delete_many({'short_code': short_code})
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'message': 'URL excluída com sucesso'
        }), 200
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

# RF5: Consultar quantidade de acessos
@app.route('/api/urls/<short_code>/stats', methods=['GET'])
def get_url_stats(short_code):
    """Retorna estatísticas de acesso de uma URL específica"""
    try:
        conn = get_mysql_connection()
        cursor = conn.cursor(dictionary=True)
        
        cursor.execute("""
            SELECT short_code, destination_url, access_count, created_at 
            FROM urls 
            WHERE short_code = %s
        """, (short_code,))
        
        url_data = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        if not url_data:
            return jsonify({
                'success': False,
                'error': 'URL curta não encontrada'
            }), 404
        
        # Converte datetime para string
        if url_data['created_at']:
            url_data['created_at'] = url_data['created_at'].isoformat()
        
        return jsonify({
            'success': True,
            'data': url_data
        }), 200
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

# RF6: Consultar histórico de acessos
@app.route('/api/urls/<short_code>/history', methods=['GET'])
def get_access_history(short_code):
    """Retorna o histórico de acessos de uma URL específica"""
    try:
        # Verifica se a URL existe no MySQL
        conn = get_mysql_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT id FROM urls WHERE short_code = %s", (short_code,))
        
        if not cursor.fetchone():
            cursor.close()
            conn.close()
            return jsonify({
                'success': False,
                'error': 'URL curta não encontrada'
            }), 404
        
        cursor.close()
        conn.close()
        
        # Busca o histórico no MongoDB
        logs = list(access_logs.find(
            {'short_code': short_code},
            {'_id': 0}
        ).sort('accessed_at', -1))
        
        return jsonify({
            'success': True,
            'data': {
                'short_code': short_code,
                'total_accesses': len(logs),
                'history': logs
            }
        }), 200
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

# RF7: Redirecionar para URL de destino
@app.route('/<short_code>', methods=['GET'])
def redirect_to_url(short_code):
    """Redireciona para a URL de destino"""
    try:
        conn = get_mysql_connection()
        cursor = conn.cursor(dictionary=True)
        
        cursor.execute("""
            SELECT destination_url 
            FROM urls 
            WHERE short_code = %s
        """, (short_code,))
        
        result = cursor.fetchone()
        
        if not result:
            cursor.close()
            conn.close()
            # RF8: Mensagem de erro para URL inexistente
            return jsonify({
                'success': False,
                'error': 'URL não encontrada',
                'message': f'A URL curta "/{short_code}" não existe ou foi removida.',
                'short_code': short_code
            }), 404
        
        destination_url = result['destination_url']
        
        # Incrementa o contador de acessos
        cursor.execute("""
            UPDATE urls 
            SET access_count = access_count + 1 
            WHERE short_code = %s
        """, (short_code,))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        # Registra o acesso no MongoDB
        client_ip = request.headers.get('X-Forwarded-For', request.remote_addr)
        
        access_logs.insert_one({
            'short_code': short_code,
            'client_ip': client_ip,
            'accessed_at': datetime.utcnow().isoformat(),
            'user_agent': request.headers.get('User-Agent', 'Unknown')
        })
        
        # Redireciona para a URL de destino
        return redirect(destination_url, code=302)
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

# Rota de health check
@app.route('/api/health', methods=['GET'])
def health_check():
    """Verifica o status da aplicação"""
    return jsonify({
        'success': True,
        'message': 'API está funcionando',
        'timestamp': datetime.utcnow().isoformat()
    }), 200

# Inicialização
if __name__ == '__main__':
    # Inicializa o banco de dados
    init_mysql_db()
    
    print("=" * 50)
    print("Encurtador de URLs - Backend")
    print("=" * 50)
    print("\nEndpoints disponíveis:")
    print("  GET    /api/health                      - Health check")
    print("  GET    /api/urls                        - Listar URLs")
    print("  POST   /api/urls                        - Criar URL curta")
    print("  DELETE /api/urls/<short_code>           - Excluir URL")
    print("  GET    /api/urls/<short_code>/stats     - Estatísticas")
    print("  GET    /api/urls/<short_code>/history   - Histórico")
    print("  GET    /<short_code>                    - Redirecionar")
    print("\n" + "=" * 50)
    
    app.run(debug=True, host='0.0.0.0', port=5000)
