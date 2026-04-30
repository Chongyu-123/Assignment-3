import socket
import sys
import os

def receive_n(sock, num_bytes):
    #get num of bytes
    data = b""
    while len(data) < num_bytes:
        chunk = sock.recv(num_bytes - len(data))
        if not chunk:
            break
        data += chunk
    return data


def main():
    if len(sys.argv) != 4:
        print("Usage: python tuple_space_client.py <server-hostname> <server-port> <input-file>")
        sys.exit(1)

    hostname = sys.argv[1]
    port = int(sys.argv[2])
    input_file_path = sys.argv[3]

    if not os.path.exists(input_file_path):
        print(f"Error: Input file '{input_file_path}' does not exist.")
        sys.exit(1)

    with open(input_file_path, 'r') as file:
        lines = file.readlines()

    # TASK 1: Create a TCP/IP socket and connect it to the server.
    # Hint: socket.socket(socket.AF_INET, socket.SOCK_STREAM) creates the socket.
    # Then call sock.connect((hostname, port)) to connect.
    
    sock = None
    try:
        #creat TCP socket
        sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        sock.connect((hostname,port)) #connect to server
        print(f"Succesefully connected to {hostname}:{port}")

        for line in lines:
            line = line.strip()
            if not line:
                continue

            parts = line.split(" ", 2)
            cmd = parts[0]
            message = ""

            # TASK 2: Build the protocol message string to send to the server.
            # Format:  "NNN X key"        for READ / GET
            #          "NNN P key value"   for PUT
            # where NNN is the total message length as a zero-padded 3-digit number,
            # X is "R" for READ and "G" for GET.
            # Hint: for READ/GET, size = 6 + len(key). For PUT, size = 7 + len(key) + len(value).
            # Reject lines with invalid format or key+" "+value > 970 chars.
            try:
                if cmd == "READ":
                    if len(parts) < 2:
                        print(f"{line}: ERR Invalid READ command format")
                        continue
                    key = parts[1]
                    total_size = 6+len(key)
                    if total_size > 999:
                        print(f"{line}: ERR key too long,message exceeds max size")
                        continue
                    message = f"{total_size:03d} R {key}"

                elif cmd == "GET":
                    if len(parts) < 2:
                        print(f"{line}: ERR Invalid GET command format")
                        continue
                    key = parts[1]
                    total_size = 6+len(key)
                    if total_size > 999:
                        print(f"{line}: ERR key too long , message exceeds max size")
                        continue
                    message = f"{total_size:03d} G {key}"

                elif cmd == "PUT":
                    if len(parts) < 3:
                        print(f"{line}: ERR key+value exceeds 970 character limit")
                        continue
                    total_size = 7 + len(key) + len(value)
                    if total_size > 999:
                        print(f"{line}: ERR Message exceeds max size")
                        continue
                    message = f"{total_size:03d} P {key} {value}"
            except Exception as e:
                print(f"{line}: ERR Failed to build message - {str(e)}")
                continue

            # TASK 3: Send the message to the server, then receive the response.
            # - Send:    sock.sendall(message.encode())
            # - Receive: first read 3 bytes to get the response size (like the server does).
            #            Then read the remaining (size - 3) bytes to get the response body.
            sock.sendall(message.encode())  #send message too the server

            resp_size_data = receive_n(sock, 3) #read 3 bytes to get total length 
            if len(resp_size_data) != 3:
                print(f"{line}: ERR Server disconnected")
                break
           
            try:   #read information left
                resp_total_size = int(resp_size_data.decode().strip())
                resp_body_data = receive_n(sock, resp_total_size - 3)
                if len(resp_body_data) != resp_total_size - 3:
                    print(f"{line}: ERR Incomplete response from server")
                    break
                response = resp_body_data.decode().strip()
                print(f"{line}: {response}")
            except ValueError:
                print(f"{line}: ERR Invalid response format from server")
                break
           

    except (socket.error, ValueError) as e:
        print(f"Error: {e}")
        sys.exit(1)
    finally:
        # TASK 4: Close the socket when done (already called for you — explain why
        # finally: is the right place to do this even if an error occurs above).
        if sock is not None:
           sock.close()

if __name__ == "__main__":
    main()