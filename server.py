import socket
import threading
import argparse
import json
import sys
import io
import datetime

from crawl import run_crawl_job, get_crawl_parser

JOB_STATUS = {}
JOB_OUTPUT = {}
JOB_LOCK = threading.Lock()
JOB_ID_COUNTER = 0

def handle_client(client_socket):
    global JOB_STATUS, JOB_OUTPUT, JOB_ID_COUNTER

    request = client_socket.recv(4096).decode('utf-8')
    try:
        command = json.loads(request)
        action = command.get('action')
        job_id = command.get('job_id')

        if action == 'start_job':
            with JOB_LOCK:
                job_id = str(JOB_ID_COUNTER)
                JOB_ID_COUNTER += 1
            args_list = command.get('args', [])
            
            # Create a new parser for each job to avoid conflicts
            job_parser = get_crawl_parser()
            try:
                args = job_parser.parse_args(args_list)
            except SystemExit as e:
                client_socket.sendall(json.dumps({'status': 'error', 'message': f'Invalid arguments: {e}'}).encode('utf-8'))
                client_socket.close()
                return

            with JOB_LOCK:
                JOB_STATUS[job_id] = {'status': 'running', 'start_time': datetime.datetime.now().isoformat()}
                JOB_OUTPUT[job_id] = []

            client_socket.sendall(json.dumps({'status': 'job_started', 'job_id': job_id}).encode('utf-8'))
            client_socket.close()

            # Run the job in a separate thread
            thread = threading.Thread(target=run_job_in_thread, args=(job_id, args))
            thread.start()

        elif action == 'get_status':
            if job_id in JOB_STATUS:
                response = {'status': 'success', 'job_status': JOB_STATUS[job_id], 'output': JOB_OUTPUT[job_id]}
            else:
                response = {'status': 'error', 'message': 'Job ID not found'}
            client_socket.sendall(json.dumps(response).encode('utf-8'))
            client_socket.close()
        else:
            client_socket.sendall(json.dumps({'status': 'error', 'message': 'Unknown action'}).encode('utf-8'))
            client_socket.close()

    except json.JSONDecodeError:
        client_socket.sendall(json.dumps({'status': 'error', 'message': 'Invalid JSON'}).encode('utf-8'))
        client_socket.close()
    except Exception as e:
        client_socket.sendall(json.dumps({'status': 'error', 'message': str(e)}).encode('utf-8'))
        client_socket.close()

def run_job_in_thread(job_id, args):
    global JOB_STATUS, JOB_OUTPUT

    def progress_callback(message):
        with JOB_LOCK:
            JOB_OUTPUT[job_id].append(f"[PROGRESS] {message}")

    old_stdout = sys.stdout
    old_stderr = sys.stderr
    redirected_output = io.StringIO()
    sys.stdout = redirected_output
    sys.stderr = redirected_output

    try:
        if args.dump:
            run_crawl_job(args, progress_callback=progress_callback)
        else:
            run_crawl_job(args)
        with JOB_LOCK:
            JOB_STATUS[job_id]['status'] = 'completed'
            JOB_STATUS[job_id]['end_time'] = datetime.datetime.now().isoformat()
    except Exception as e:
        with JOB_LOCK:
            JOB_STATUS[job_id]['status'] = 'failed'
            JOB_STATUS[job_id]['error'] = str(e)
            JOB_STATUS[job_id]['end_time'] = datetime.datetime.now().isoformat()
    finally:
        with JOB_LOCK:
            JOB_OUTPUT[job_id].extend(redirected_output.getvalue().splitlines())
        sys.stdout = old_stdout
        sys.stderr = old_stderr

def main():
    server_parser = argparse.ArgumentParser(description='Wikidot Crawler Socket Server')
    server_parser.add_argument('--host', type=str, default='127.0.0.1', help='Host to bind to')
    server_parser.add_argument('--port', type=int, default=12345, help='Port to listen on')
    server_args = server_parser.parse_args()

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((server_args.host, server_args.port))
    server.listen(5)

    print(f"[*] Listening on {server_args.host}:{server_args.port}")

    while True:
        client_sock, address = server.accept()
        print(f"[*] Accepted connection from {address[0]}:{address[1]}")
        client_handler = threading.Thread(target=handle_client, args=(client_sock,))
        client_handler.start()

if __name__ == '__main__':
    main()
