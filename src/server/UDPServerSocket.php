<?php

/*
 *
 *  ____            _        _   __  __ _                  __  __ ____  
 * |  _ \ ___   ___| | _____| |_|  \/  (_)_ __   ___      |  \/  |  _ \ 
 * | |_) / _ \ / __| |/ / _ \ __| |\/| | | '_ \ / _ \_____| |\/| | |_) |
 * |  __/ (_) | (__|   <  __/ |_| |  | | | | | |  __/_____| |  | |  __/ 
 * |_|   \___/ \___|_|\_\___|\__|_|  |_|_|_| |_|\___|     |_|  |_|_| 
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * @author PocketMine Team
 * @link http://www.pocketmine.net/
 * 
 *
*/

namespace pocketraknet\server;

use pocketraknet\utils\Logger;

class UDPServerSocket{
    /**
     * Returned by readPacket() when the read failed for a reason that says nothing about the
     * datagrams still queued on the socket. Distinct from false (socket empty) so the caller
     * can keep draining instead of ending its receive loop for the tick.
     */
    const RECV_IGNORED_ERROR = -1;

    /** @var Logger */
    protected $logger;
    protected $socket;

    public function __construct(Logger $logger, $port = 19132, $interface = "0.0.0.0"){
        $this->logger = $logger;
        $this->socket = socket_create(AF_INET, SOCK_DGRAM, SOL_UDP);
        //socket_set_option($this->socket, SOL_SOCKET, SO_BROADCAST, 1); //Allow sending broadcast messages
        if(@socket_bind($this->socket, $interface, $port) === true){
            socket_set_option($this->socket, SOL_SOCKET, SO_REUSEADDR, 0);
            $this->setSendBuffer(1024 * 1024 * 8)->setRecvBuffer(1024 * 1024 * 8);
        }else{
            $logger->critical("**** FAILED TO BIND TO " . $interface . ":" . $port . "!", true, true, 0);
            $logger->critical("Perhaps a server is already running on that port?", true, true, 0);
            exit(1);
        }
        socket_set_nonblock($this->socket);
    }

    public function getSocket(){
        return $this->socket;
    }

    public function close(){
        socket_close($this->socket);
    }

    /**
     * @param string &$buffer
     * @param string &$source
     * @param int    &$port
     *
     * @return int|bool Bytes read, false if the socket had nothing to give, or
     *                  self::RECV_IGNORED_ERROR if the read failed for an ignorable reason.
     */
    public function readPacket(&$buffer, &$source, &$port){
        $len = @socket_recvfrom($this->socket, $buffer, 65535, 0, $source, $port);
        if($len === false){
            $errno = socket_last_error($this->socket);
            socket_clear_error($this->socket);
            if($errno === SOCKET_ECONNRESET){
                //Windows only. A socket_sendto() to a peer that had already closed its socket draws
                //an ICMP Port Unreachable, and Windows reports it on the NEXT recvfrom() on this
                //socket instead of dropping it. It concerns a peer we were writing to, not the
                //datagrams queued here, so the caller must keep draining. Dead sessions are reaped
                //by their own timeout.
                return self::RECV_IGNORED_ERROR;
            }
            if($errno !== SOCKET_EWOULDBLOCK){ //EWOULDBLOCK is just an empty non-blocking socket
                $this->logger->debug("Failed to recv (errno $errno): " . trim(socket_strerror($errno)));
            }
        }
        return $len;
    }

    /**
     * @param string $buffer
     * @param string $dest
     * @param int    $port
     *
     * @return int
     */
    public function writePacket($buffer, $dest, $port){
        $result = @socket_sendto($this->socket, $buffer, strlen($buffer), 0, $dest, $port);
        if($result === false){
            $errno = socket_last_error($this->socket);
            socket_clear_error($this->socket);
            //Same ICMP Port Unreachable as in readPacket(), surfacing on the send side when the
            //peer went away between two writes. Nothing to do about it here either.
            if($errno !== SOCKET_ECONNRESET and $errno !== SOCKET_EWOULDBLOCK){
                $this->logger->debug("Failed to send to $dest $port (errno $errno): " . trim(socket_strerror($errno)));
            }
            return 0; //callers add the result to their sent-byte counters
        }
        return $result;
    }

    /**
     * @param int $size
     *
     * @return $this
     */
    public function setSendBuffer($size){
        @socket_set_option($this->socket, SOL_SOCKET, SO_SNDBUF, $size);

        return $this;
    }

    /**
     * @param int $size
     *
     * @return $this
     */
    public function setRecvBuffer($size){
        @socket_set_option($this->socket, SOL_SOCKET, SO_RCVBUF, $size);

        return $this;
    }

}

?>