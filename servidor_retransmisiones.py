# -*- coding: utf-8 -*-
"""
Script para responder a solicitudes
de retransmisión estilo BMV

Julián Sánchez Mena
2022-10-28
"""

import socket
from time import time

# Constants
headerSize = 17
lengthSize = 2

# Structures
loginResponse = bytearray(headerSize + lengthSize + 2)
replayResponse = bytearray(headerSize + lengthSize + 9)
replayPacket = bytearray(headerSize + lengthSize + 52)


def main_loop():

    while True:

        # Wait for a connection
        print('Esperando por una conexión...')
        connection, client_address = sock.accept()

        try:
            print('Conexión de cliente desde: %s' % str(client_address[0]))

            # Receive the data in small chunks and retransmit it
            while True:
                data = connection.recv(19)
                if data:
                    print('Información recibida analizando...')
                    if data[0] != 19:
                        print('El tamaño de los datos no es el esperado [%d] ignoramos al cliente' % str(data[0]))
                        print('Cerramos la conexión...')
                        connection.close()
                        break

                    if data[1] != 33:
                        print('El tipo de mensaje no es el esperado [%d] ignoramos al cliente' % str(data[1]))
                        print('Cerramos la conexión...')
                        connection.close()
                        break

                    if data[2] != 18:
                        print('El código de grupo no es el esperado [%d] respondemos al cliente B' % str(data[2]))
                        fill_login_response('B')
                        connection.sendall(loginResponse)
                        print('Cerramos la conexión...')
                        connection.close()
                        break

                    print(
                        'Solicitud de sesion grupo: %d usuario: %s, passw: %s' % (data[2], str(data[3:9]), str(data[9:19])))
                    print('Respondemos al cliente A')
                    fill_login_response('A')
                    connection.sendall(loginResponse)

                    data2 = connection.recv(9)

                    if data2:
                        print('Información recibida nuevamente, analizando...')
                        if data2[0] != 9:
                            print('El tamaño de los datos no es el esperado [%d] ignoramos al cliente' % str(data2[0]))
                            print('Cerramos la conexión...')
                            connection.close()
                            break
                        if data2[1] != 35:
                            print('El tipo de mensaje no es el esperado [%d] ignoramos al cliente' % str(data2[1]))
                            print('Cerramos la conexión...')
                            connection.close()
                            break

                        if data2[2] != 18:
                            print('El código de grupo no es el esperado [%d] respondemos al cliente B' % str(data2[2]))
                            fill_replay_response('B', 0, 0, 0)
                            connection.sendall(replayResponse)
                            print('Cerramos la conexión...')
                            connection.close()
                            break

                        firstMessageArray = data2[3:7]
                        firstMessage = int.from_bytes(firstMessageArray, 'big')

                        if firstMessage < 0:
                            print('El primer mensaje no es válido [%d] respondemos al cliente J' % firstMessage)
                            fill_replay_response('J', 0, 0, 0)
                            connection.sendall(replayResponse)
                            print('Cerramos la conexión...')
                            connection.close()
                            break

                        quantityArray = data2[7:9]
                        quantity = int.from_bytes(quantityArray, 'big')

                        if quantity < 0:
                            print('La cantidad mensaje no es válida [%d] respondemos al cliente K' % quantity)
                            fill_replay_response('K', 0, 0, 0)
                            connection.sendall(replayResponse)
                            print('Cerramos la conexión...')
                            connection.close()
                            break

                        print('Solicitud de retransmision, grupo: %d, primera secuencia: %d, cantidad: %d' % (
                            data2[2], firstMessage, quantity))
                        print('Respondemos al cliente solicitud aceptada A')
                        fill_replay_response('A', data2[2], firstMessage, quantity)
                        connection.sendall(replayResponse)

                        # Aquí enviamos los paquetes
                        for i in range(firstMessage, firstMessage + quantity):
                            fill_replay_packet(i)
                            print('Enviando paquete con secuencia inicial: %d' % i)
                            connection.sendall(replayPacket)
                        print('Cerramos la conexión...')
                        connection.close()
                        break
                else:
                    print('No se obtuvieron más datos de: %s' % str(client_address[0]))
                    break
        except (RuntimeError, TypeError, NameError):
            print('Ha ocurrido un error. terminamos la aplicación')
            return


def fill_login_response(responseStatus):
    # HEADER
    # Length
    length = headerSize + lengthSize + 2
    loginResponse[0:2] = length.to_bytes(2, 'big')
    # Total Messages
    loginResponse[2] = 1
    # Market Data Group
    loginResponse[3] = 18
    # Session
    loginResponse[4] = 2
    # Sequence Number
    sequenceNumber = 0
    loginResponse[5:9] = sequenceNumber.to_bytes(4, 'big')
    # Date-Time
    loginResponse[9:17] = (int(time()) * 1000 + 123).to_bytes(8, 'big')

    # LENGTH
    # Length message
    loginResponse[headerSize:headerSize + lengthSize] = (int(2)).to_bytes(2, 'big')

    # MESSAGE
    loginResponse[headerSize + lengthSize + 0] = 38  # &
    loginResponse[headerSize + lengthSize + 1] = str.encode(responseStatus, 'iso_8859_1')[0]
    return


def fill_replay_response(replayStatus, group, firstMessage, quantity):
    # HEADER
    # Length
    length = headerSize + lengthSize + 9
    replayResponse[0:2] = length.to_bytes(2, 'big')
    # Total Messages
    replayResponse[2] = 1
    # Market Data Group
    replayResponse[3] = 18
    # Session
    replayResponse[4] = 2
    # Sequence Number
    sequenceNumber = 0
    replayResponse[5:9] = sequenceNumber.to_bytes(4, 'big')
    # Date-Time
    replayResponse[9:17] = (int(time()) * 1000 + 123).to_bytes(8, 'big')

    # LENGTH
    # Length message
    replayResponse[headerSize:headerSize + lengthSize] = (int(2)).to_bytes(2, 'big')

    # MESSAGE
    replayResponse[headerSize + lengthSize + 0] = 42  # *
    replayResponse[headerSize + lengthSize + 1] = group
    firstMessageArray = firstMessage.to_bytes(4, 'big')
    replayResponse[headerSize + lengthSize + 2:headerSize + lengthSize + 6] = firstMessageArray
    quantityArray = quantity.to_bytes(2, 'big')
    replayResponse[headerSize + lengthSize + 6:headerSize + lengthSize + 8] = quantityArray
    replayResponse[headerSize + lengthSize + 8] = str.encode(replayStatus, 'iso_8859_1')[0]
    return


def fill_replay_packet(sequence):
    # HEADER
    # Length
    length = headerSize + lengthSize + 52
    replayPacket[0:2] = length.to_bytes(2, 'big')
    # Total Messages
    replayPacket[2] = 1
    # Market Data Group
    replayPacket[3] = 18
    # Session
    replayPacket[4] = 2
    # Sequence Number
    sequenceNumber = sequence
    replayPacket[5:9] = sequenceNumber.to_bytes(4, 'big')
    # Date-Time
    replayPacket[9:17] = (int(time()) * 1000 + 123).to_bytes(8, 'big')

    # LENGTH
    # Length message
    replayPacket[headerSize:headerSize + lengthSize] = (int(52)).to_bytes(2, 'big')

    # MESSAGE
    # Message type
    replayPacket[headerSize + lengthSize + 0] = 80  # P
    # Instrument Number
    replayPacket[headerSize + lengthSize + 1:headerSize + lengthSize + 5] = (int(12345)).to_bytes(4, 'big')
    # Trade Time
    replayPacket[headerSize + lengthSize + 5:headerSize + lengthSize + 13] = (int(time()) * 1000).to_bytes(8, 'big')
    # Volume
    replayPacket[headerSize + lengthSize + 13:headerSize + lengthSize + 17] = (int(200)).to_bytes(4, 'big')
    # Price
    replayPacket[headerSize + lengthSize + 17:headerSize + lengthSize + 25] = (int(1050000000)).to_bytes(8, 'big')
    # Concertation type
    replayPacket[headerSize + lengthSize + 25] = 67  # C
    # Trade Number
    replayPacket[headerSize + lengthSize + 26:headerSize + lengthSize + 30] = sequenceNumber.to_bytes(4, 'big')
    # Price Setter
    replayPacket[headerSize + lengthSize + 30] = 1
    # Operation type
    replayPacket[headerSize + lengthSize + 31] = 67  # C
    # Amount
    replayPacket[headerSize + lengthSize + 32:headerSize + lengthSize + 40] = (int(210000000000)).to_bytes(8, 'big')
    # Buy
    buyStr = 'GBM  '
    buyArray = str.encode(buyStr, 'iso_8859_1')
    replayPacket[headerSize + lengthSize + 40:headerSize + lengthSize + 45] = buyArray
    # Sell
    sellStr = 'HSBC '
    sellArray = str.encode(sellStr, 'iso_8859_1')
    replayPacket[headerSize + lengthSize + 45:headerSize + lengthSize + 50] = sellArray
    # Settlement
    replayPacket[headerSize + lengthSize + 50] = 50  # 2
    # Auction indicator
    replayPacket[headerSize + lengthSize + 50] = 32  # space
    return


# Create a TCP/IP socket
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

# Bind the socket to the port
port = 10000
server_address = ('localhost', port)
print('Escuchando en el puerto: %d' % port)
sock.bind(server_address)

# Listen for incoming connections
sock.listen(1)
main_loop()

