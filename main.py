#!/usr/bin/env python3
import socket
import argparse
import json
import os
import hashlib
import time
import threading
import uuid
from rich.progress import Progress

CHUNK_SIZE = 4096

# グローバル辞書：parallel transfer の進捗管理用
parallel_transfers = {}

def send_json(sock, data):
    """
    JSONを1行（改行区切り）で送信するヘルパー関数
    """
    line = json.dumps(data) + "\n"
    sock.sendall(line.encode('utf-8'))

def recv_json(sock):
    """
    改行まで受信してJSONオブジェクトに変換するヘルパー関数
    """
    buffer = b""
    while True:
        chunk = sock.recv(1)
        if not chunk:
            break
        if chunk == b'\n':
            break
        buffer += chunk
    if not buffer:
        return None
    return json.loads(buffer.decode('utf-8'))

def compute_checksum(file_path):
    """
    ファイルのSHA256チェックサムを計算
    """
    sha256 = hashlib.sha256()
    with open(file_path, 'rb') as f:
        while True:
            data = f.read(CHUNK_SIZE)
            if not data:
                break
            sha256.update(data)
    return sha256.hexdigest()

# =======================
# シーケンシャル転送（既存実装）用関数
# =======================

def send_file(sock, file_path):
    """
    ファイル送信（エラー訂正機能付き・シーケンシャル版）
    """
    filesize = os.path.getsize(file_path)
    filename = os.path.basename(file_path)
    checksum = compute_checksum(file_path)
    
    while True:
        # ヘッダー送信
        header = {"filename": filename, "filesize": filesize, "mode": "file_transfer"}
        send_json(sock, header)
        
        # ファイルデータ送信
        with open(file_path, 'rb') as f:
            with Progress() as progress:
                task = progress.add_task("Sending...", total=filesize)
                while True:
                    data = f.read(CHUNK_SIZE)
                    if not data:
                        break
                    sock.sendall(data)
                    progress.update(task, advance=len(data))
        
        # チェックサム送信
        send_json(sock, {"checksum": checksum})
        
        # 受信側からの ack 待ち
        ack = recv_json(sock)
        if ack and ack.get("ack") == "OK":
            print("File sent successfully.")
            break
        else:
            print("Checksum error detected. Resending file...")
            time.sleep(1)

def sequential_receive_file(sock, header, output_dir="."):
    """
    ファイル受信（エラー訂正機能付き・シーケンシャル版）
    headerは既に受信済みのものとする
    """
    filename = header.get("filename")
    filesize = header.get("filesize")
    out_path = os.path.join(output_dir, filename)
    
    received_bytes = 0
    sha256 = hashlib.sha256()
    with open(out_path, 'wb') as f:
        with Progress() as progress:
            task = progress.add_task("Receiving...", total=filesize)
            while received_bytes < filesize:
                remaining = filesize - received_bytes
                data = sock.recv(min(CHUNK_SIZE, remaining))
                if not data:
                    break
                f.write(data)
                sha256.update(data)
                received_bytes += len(data)
                progress.update(task, advance=len(data))
    
    # チェックサムの受信
    data = recv_json(sock)
    if data:
        sender_checksum = data.get("checksum")
        local_checksum = sha256.hexdigest()
        if local_checksum == sender_checksum:
            send_json(sock, {"ack": "OK"})
            print(f"Received file '{filename}' successfully.")
        else:
            send_json(sock, {"ack": "RESEND"})
            print("Checksum mismatch. Requesting resend...")
            try:
                os.remove(out_path)
            except Exception:
                pass

# =======================
# 並列転送用関数（drop/send側）
# =======================

def send_file_parallel(target_ip, port, file_path, num_threads):
    """
    並列転送でのファイル送信（drop send 用）
    
    ・まずコントロール接続で転送情報（ファイル名、サイズ、全体チェックサム、セグメント数、転送ID）を送信  
    ・その後、ファイルを num_threads 個のセグメントに分割し、
      各スレッドで新たな接続を確立し、セグメント転送を行う。
    ・全セグメント送信後、コントロール接続で ack を受信。
    """
    filesize = os.path.getsize(file_path)
    filename = os.path.basename(file_path)
    overall_checksum = compute_checksum(file_path)
    transfer_id = str(uuid.uuid4())
    
    # コントロール接続の確立
    sock_control = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock_control.connect((target_ip, port))
    control_header = {
        "filename": filename,
        "filesize": filesize,
        "mode": "parallel_file_transfer",
        "threads": num_threads,
        "transfer_id": transfer_id,
        "checksum": overall_checksum
    }
    send_json(sock_control, control_header)
    
    # ファイルをセグメントに分割
    segment_size = filesize // num_threads
    segments = []
    for i in range(num_threads):
        offset = i * segment_size
        # 最後のセグメントは余りを含む
        if i == num_threads - 1:
            seg_size = filesize - offset
        else:
            seg_size = segment_size
        segments.append((offset, seg_size))
    
    threads = []
    def send_segment(offset, seg_size):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((target_ip, port))
            seg_header = {
                "transfer_id": transfer_id,
                "mode": "parallel_data",
                "offset": offset,
                "segment_size": seg_size
            }
            send_json(sock, seg_header)
            with open(file_path, 'rb') as f:
                f.seek(offset)
                bytes_sent = 0
                while bytes_sent < seg_size:
                    data = f.read(min(CHUNK_SIZE, seg_size - bytes_sent))
                    if not data:
                        break
                    sock.sendall(data)
                    bytes_sent += len(data)
            sock.close()
        except Exception as e:
            print(f"Error sending segment at offset {offset}: {e}")
    
    # 各セグメント送信を並列で実施
    for offset, seg_size in segments:
        t = threading.Thread(target=send_segment, args=(offset, seg_size))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()
    
    # コントロール接続で ack 待ち
    ack = recv_json(sock_control)
    if ack and ack.get("ack") == "OK":
        print("File sent successfully via parallel transfer.")
    else:
        print("Checksum error detected in parallel transfer.")
    sock_control.close()

# drop send で並列/シーケンシャルの切り替え用
def drop_mode_send(target_ip, port, file_path, num_threads):
    if num_threads > 1:
        send_file_parallel(target_ip, port, file_path, num_threads)
    else:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((target_ip, port))
        send_file(sock, file_path)
        sock.close()

# =======================
# 並列転送用受信処理（receiver 側）
# =======================

def handle_parallel_control(conn, header, output_dir="."):
    """
    並列転送時のコントロール接続用ハンドラ  
    ヘッダーにはファイル名、サイズ、転送ID、セグメント数、全体チェックサムが含まれている。
    受信側はあらかじめ指定サイズの空ファイルを作成し、グローバル辞書に転送情報を登録。
    その後、全セグメント受信完了まで待機し、全体チェックサム検証後に ack を返す。
    """
    filename = header.get("filename")
    filesize = header.get("filesize")
    num_threads = header.get("threads")
    transfer_id = header.get("transfer_id")
    expected_checksum = header.get("checksum")
    out_path = os.path.join(output_dir, filename)
    
    # 受信用に空ファイルを確保
    with open(out_path, 'wb') as f:
        f.truncate(filesize)
    
    global parallel_transfers
    parallel_transfers[transfer_id] = {
        "filename": out_path,
        "filesize": filesize,
        "threads": num_threads,
        "checksum": expected_checksum,
        "received_segments": 0,
        "lock": threading.Lock(),
        "done_event": threading.Event()
    }
    print(f"Started parallel transfer for '{filename}' with {num_threads} segments.")
    
    # 全セグメント受信完了まで待機
    parallel_transfers[transfer_id]["done_event"].wait()
    
    # 全セグメント受信後、チェックサム検証
    sha256 = hashlib.sha256()
    with open(out_path, 'rb') as f:
        while True:
            data = f.read(CHUNK_SIZE)
            if not data:
                break
            sha256.update(data)
    local_checksum = sha256.hexdigest()
    if local_checksum == expected_checksum:
        send_json(conn, {"ack": "OK"})
        print(f"Received file '{filename}' successfully via parallel transfer.")
    else:
        send_json(conn, {"ack": "RESEND"})
        print("Checksum mismatch in parallel transfer. Requesting resend...")
        try:
            os.remove(out_path)
        except Exception:
            pass
    # 後処理
    del parallel_transfers[transfer_id]
    conn.close()

def handle_parallel_data(conn, header, output_dir="."):
    """
    並列転送時の各セグメントを受信するハンドラ  
    ヘッダーには転送ID、オフセット、セグメントサイズが含まれている。
    指定オフセットに対して受信データを書き込む。
    """
    transfer_id = header.get("transfer_id")
    offset = header.get("offset")
    segment_size = header.get("segment_size")
    if not transfer_id or offset is None or segment_size is None:
        print("Invalid parallel data header.")
        conn.close()
        return
    global parallel_transfers
    if transfer_id not in parallel_transfers:
        print("Transfer ID not found for parallel data.")
        conn.close()
        return
    transfer_info = parallel_transfers[transfer_id]
    filename = transfer_info["filename"]
    
    received = 0
    with open(filename, 'r+b') as f:
        f.seek(offset)
        while received < segment_size:
            data = conn.recv(min(CHUNK_SIZE, segment_size - received))
            if not data:
                break
            f.write(data)
            received += len(data)
    
    # 受信セグメント数を更新
    with transfer_info["lock"]:
        transfer_info["received_segments"] += 1
        if transfer_info["received_segments"] == transfer_info["threads"]:
            transfer_info["done_event"].set()
    conn.close()

# =======================
# 接続ハンドラ（受信側共通）
# =======================

def handle_connection(conn, output_dir="."):
    """
    受信側の接続ハンドラ  
    先頭のJSONヘッダーを解析し、モードに応じてシーケンシャル/並列の処理を振り分ける。
    """
    header = recv_json(conn)
    if not header:
        print("No header received.")
        conn.close()
        return
    mode = header.get("mode")
    if mode == "file_transfer":
        sequential_receive_file(conn, header, output_dir)
    elif mode == "parallel_file_transfer":
        handle_parallel_control(conn, header, output_dir)
    elif mode == "parallel_data":
        handle_parallel_data(conn, header, output_dir)
    else:
        print("Unknown mode.")
        conn.close()

# =======================
# サーバー側（drop モード・get モード）処理
# =======================

def drop_mode_receive(port, output_dir="."):
    """
    Dropモード（Receive）：常に待機して接続してきたファイルを受信  
    各接続は新たなスレッドで処理する
    """
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.bind(("", port))
    server_sock.listen(5)
    print(f"Drop mode receiver listening on port {port}...")
    while True:
        conn, addr = server_sock.accept()
        print(f"Connection from {addr}")
        t = threading.Thread(target=handle_connection, args=(conn, output_dir))
        t.start()

def get_mode_server(port, directory):
    """
    Getモード（Server）：指定ディレクトリ内のファイルを保持し、
    クライアントのGETリクエストに応じて送信（シーケンシャルのみ対応）
    """
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.bind(("", port))
    server_sock.listen(5)
    print(f"Get mode server listening on port {port}...")
    while True:
        conn, addr = server_sock.accept()
        print(f"Connection from {addr}")
        def handle_get(conn):
            try:
                request = recv_json(conn)
                if request and request.get("request") == "GET":
                    filename = request.get("filename")
                    file_path = os.path.join(directory, filename)
                    if os.path.isfile(file_path):
                        num_threads = request.get("threads", 1)
                        if num_threads > 1:
                            print("Parallel transfer not supported in GET server mode. Using sequential transfer.")
                            send_file(conn, file_path)
                        else:
                            send_file(conn, file_path)
                    else:
                        send_json(conn, {"error": "File not found"})
            except Exception as e:
                print(f"Error in server: {e}")
            finally:
                conn.close()
        t = threading.Thread(target=handle_get, args=(conn,))
        t.start()

# =======================
# クライアント側（GET モード）処理
# =======================

def get_mode_client(server_ip, port, filename, output_dir=".", num_threads=1):
    """
    Getモード（Client）：サーバーに対してGETリクエストを送り、ファイルを受信  
    ※並列転送の実装はここでは行わず、num_threads>1の場合はシーケンシャルにフォールバック
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((server_ip, port))
    request = {"request": "GET", "filename": filename}
    if num_threads > 1:
        request["threads"] = num_threads
    send_json(sock, request)
    
    header = recv_json(sock)
    if header is None:
        print("No response from server.")
        sock.close()
        return

    if header.get("mode") != "file_transfer":
        if "error" in header:
            print(f"Error: {header['error']}")
        else:
            print("Unexpected response from server.")
        sock.close()
        return

    recv_filename = header.get("filename")
    filesize = header.get("filesize")
    out_path = os.path.join(output_dir, recv_filename)
    received_bytes = 0
    sha256 = hashlib.sha256()
    with open(out_path, 'wb') as f:
        with Progress() as progress:
            task = progress.add_task("Receiving...", total=filesize)
            while received_bytes < filesize:
                remaining = filesize - received_bytes
                data = sock.recv(min(CHUNK_SIZE, remaining))
                if not data:
                    break
                f.write(data)
                sha256.update(data)
                received_bytes += len(data)
                progress.update(task, advance=len(data))
    data = recv_json(sock)
    if data:
        sender_checksum = data.get("checksum")
        local_checksum = sha256.hexdigest()
        if local_checksum == sender_checksum:
            send_json(sock, {"ack": "OK"})
            print(f"Received file '{recv_filename}' successfully.")
        else:
            send_json(sock, {"ack": "RESEND"})
            print("Checksum mismatch. Transfer failed.")
    sock.close()

# =======================
# メイン処理
# =======================

def main():
    parser = argparse.ArgumentParser(description="USFTP (Ultra-speed File Transfer Protocol)")
    parser.add_argument("--mode", choices=["drop", "get"], required=True,
                        help="モード: drop (Dropモード) または get (Getモード)")
    parser.add_argument("--role", choices=["receive", "send", "server", "client"], required=True,
                        help="役割: dropの場合は receive/send、getの場合は server/client")
    parser.add_argument("--file", help="送信するファイルパス（drop send）または取得するファイル名（get client）")
    parser.add_argument("--target_ip", help="drop sendモード時の送信先IPアドレス")
    parser.add_argument("--server_ip", help="get clientモード時のサーバーIPアドレス")
    parser.add_argument("--port", type=int, help="ポート番号", default=28380)
    parser.add_argument("--dir", help="get serverモード時のファイル保管ディレクトリ", default=".")
    parser.add_argument("--output_dir", help="受信ファイルの出力ディレクトリ", default=".")
    parser.add_argument("--threads", type=int, help="ファイル転送時のスレッド数（1ならシーケンシャル、2以上なら並列転送）", default=1)
    args = parser.parse_args()
    
    if args.mode == "drop":
        if args.role == "receive":
            drop_mode_receive(args.port, args.output_dir)
        elif args.role == "send":
            if not args.file or not args.target_ip:
                print("drop sendモードでは、--file と --target_ip が必須です。")
                return
            drop_mode_send(args.target_ip, args.port, args.file, args.threads)
    elif args.mode == "get":
        if args.role == "server":
            get_mode_server(args.port, args.dir)
        elif args.role == "client":
            if not args.file or not args.server_ip:
                print("get clientモードでは、--file と --server_ip が必須です。")
                return
            get_mode_client(args.server_ip, args.port, args.file, args.output_dir, num_threads=args.threads)

if __name__ == "__main__":
    main()
