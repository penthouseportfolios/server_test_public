
import socket

host = 'local host'
port = 9000

s = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
s.bind(('',port))
s.listen(3)
print ('Connecting...')
c,addr = s.accept()
print ('Connected to',str(addr))
while True:
    msg = 'works'
    c.send(msg.encode())
c.close()