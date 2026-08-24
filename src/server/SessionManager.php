<?php

/*
 * RakLib network library
 *
 *
 * This project is not affiliated with Jenkins Software LLC nor RakNet.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 */

namespace pocketraknet\server;

use pocketraknet\Binary;
use pocketraknet\protocol\ACK;
use pocketraknet\protocol\ADVERTISE_SYSTEM;
use pocketraknet\protocol\DATA_PACKET_0;
use pocketraknet\protocol\DATA_PACKET_1;
use pocketraknet\protocol\DATA_PACKET_2;
use pocketraknet\protocol\DATA_PACKET_3;
use pocketraknet\protocol\DATA_PACKET_4;
use pocketraknet\protocol\DATA_PACKET_5;
use pocketraknet\protocol\DATA_PACKET_6;
use pocketraknet\protocol\DATA_PACKET_7;
use pocketraknet\protocol\DATA_PACKET_8;
use pocketraknet\protocol\DATA_PACKET_9;
use pocketraknet\protocol\DATA_PACKET_A;
use pocketraknet\protocol\DATA_PACKET_B;
use pocketraknet\protocol\DATA_PACKET_C;
use pocketraknet\protocol\DATA_PACKET_D;
use pocketraknet\protocol\DATA_PACKET_E;
use pocketraknet\protocol\DATA_PACKET_F;
use pocketraknet\protocol\EncapsulatedPacket;
use pocketraknet\protocol\INCOMPATIBLE_PROTOCOL_VERSION;
use pocketraknet\protocol\NACK;
use pocketraknet\protocol\OPEN_CONNECTION_REPLY_1;
use pocketraknet\protocol\OPEN_CONNECTION_REPLY_2;
use pocketraknet\protocol\OPEN_CONNECTION_REQUEST_1;
use pocketraknet\protocol\OPEN_CONNECTION_REQUEST_2;
use pocketraknet\protocol\Packet;
use pocketraknet\protocol\UNCONNECTED_PING;
use pocketraknet\protocol\UNCONNECTED_PING_OPEN_CONNECTIONS;
use pocketraknet\protocol\UNCONNECTED_PONG;
use pocketraknet\RakLib;

class SessionManager{
    protected $packetPool = [];

    /** @var RakLibServer */
    protected $server;

    protected $socket;

    protected $receiveBytes = 0;
    protected $sendBytes = 0;

    /** @var Session[] */
    protected $sessions = [];

    protected $name = "";

    private const RAKLIB_TPS = 100;
    private const RAKLIB_TIME_PER_TICK = 1 / self::RAKLIB_TPS;
    protected $packetLimit = 200;

    /**
     * Ceiling on datagrams accepted per tick, all sources together.
     * The per-address counter is useless against a flood with forged sources: every fake
     * address starts with a fresh budget, so only a global count caps it.
     *
     * The usable range is narrow, and measured rather than guessed. The receive loop is
     * already bounded at 5000 datagrams between two ticks, so any value at or above that
     * can never fire. At the low end, 120 clients connecting at once were measured
     * peaking at 2271 datagrams in a single tick - a legitimate burst, and 2000 cut into
     * it, turning honest traffic into peer retransmissions. 4000 sits between the two.
     */
    protected $globalPacketLimit = 4000;
    protected $globalCount = 0;
    protected $globalDropped = 0;

    /**
     * Half-open handshakes, as "address:port" => time of the REQUEST_1.
     * An OPEN_CONNECTION_REQUEST_1 used to allocate a full Session straight away, so any
     * source could make the server build one and keep it until it timed out. A pending
     * entry is a single float instead, it is capped, and it expires on its own.
     */
    protected $pendingConnections = [];
    protected $maxPendingConnections = 1024;
    private const PENDING_TIMEOUT = 10;

    protected $shutdown = false;

    protected $ticks = 0;
    protected $lastMeasure;

    protected $block = [];
    protected $ipSec = [];

    /**
     * Addresses that blockAddress() never blocks, as address => true.
     * The per-address counter cannot tell one flooding client from several sharing
     * the same address, so blocking cuts them all at once. Loopback can only be a
     * test rig, never an attacker worth blocking.
     */
    protected $blockExceptions = ["127.0.0.1" => true, "::1" => true];

    protected $pingTimes = [];

    protected $serverId = 0;

    public $portChecking = true;

    public function __construct(RakLibServer $server, UDPServerSocket $socket){
        $this->server = $server;
        $this->socket = $socket;
        $this->registerPackets();

	    $this->serverId = mt_rand(0, PHP_INT_MAX);

        $this->run();
    }

    public function getPort(){
        return $this->server->getPort();
    }

    public function getLogger(){
        return $this->server->getLogger();
    }

    public function run(){
        $this->tickProcessor();
    }

    private function sessionExists($ip, $port)
    {
        return isset($this->sessions[$ip.":".$port]);
    }

    public function getSessionByClientID($clientID)
    {
        foreach ($this->sessions as $session) {
            if ($session !== null && $session->getID() == $clientID) {
                return $session;
            }
        }
        return null;
    }

    private function tickProcessor(){
        $this->lastMeasure = microtime(true);

        while(!$this->shutdown){
            try{
                $start = microtime(true);
                $max = 5000;
                while(--$max and $this->receivePacket());
    	        while($this->receiveStream());
    			$time = microtime(true) - $start;
                if($time < self::RAKLIB_TIME_PER_TICK){
                    usleep((int) ((self::RAKLIB_TIME_PER_TICK - $time) * 1000000));
                }
    			$this->tick();
            }catch(\Throwable $e){
                //A single malformed packet or a bad session must never take down the whole RakLib thread.
                //Log and keep ticking instead of letting the exception unwind out of run().
                $logger = $this->server->getLogger();
                $logger->logException($e);
            }
        }
    }

	private function tick(){
		$time = microtime(true);
		foreach($this->sessions as $session){
			$session->update($time);
		}

		foreach($this->ipSec as $address => $count){
			if($count >= $this->packetLimit){
				$this->blockAddress($address);
			}
		}
		$this->ipSec = [];
		$this->globalCount = 0;
		if($this->globalDropped > 0){
			//Never drop silently: a dropped datagram becomes a peer retransmission, which is
			//otherwise indistinguishable from ordinary packet loss when reading a load test.
			$this->getLogger()->notice("Global packet limit reached: dropped " . $this->globalDropped . " datagrams in one tick (limit " . $this->globalPacketLimit . ")");
			$this->globalDropped = 0;
		}

		if($this->pendingConnections !== []){
			$expiry = $time - self::PENDING_TIMEOUT;
			foreach($this->pendingConnections as $pendingId => $since){
				if($since < $expiry){
					unset($this->pendingConnections[$pendingId]);
				}
			}
		}



		if(($this->ticks % self::RAKLIB_TPS) === 0){
			$diff = max(0.005, $time - $this->lastMeasure);
			$this->streamOption("bandwidth", serialize([
				"up" => $this->sendBytes / $diff,
				"down" => $this->receiveBytes / $diff
			]));
			$this->lastMeasure = $time;
			$this->sendBytes = 0;
			$this->receiveBytes = 0;

			if(count($this->block) > 0){
				asort($this->block);
				$now = microtime(true);
				foreach($this->block as $address => $timeout){
					if($timeout <= $now){
						unset($this->block[$address]);
					}else{
						break;
					}
				}
			}
		}

		++$this->ticks;
	}


    private function receivePacket(){
        $len = $this->socket->readPacket($buffer, $source, $port);
        if($len === UDPServerSocket::RECV_IGNORED_ERROR){
            //Not a real read failure and not an empty socket: say "handled" so tickProcessor()
            //keeps draining this tick instead of stopping on someone else's dead peer.
            return true;
        }
        if($len > 0){
            $this->receiveBytes += $len;
            if(isset($this->block[$source])){
                return true;
            }

            if(++$this->globalCount > $this->globalPacketLimit){
                ++$this->globalDropped;
                return true; //drop before parsing; the loop keeps draining the socket
            }

            if(isset($this->ipSec[$source])){
                $this->ipSec[$source]++;
            }else{
                $this->ipSec[$source] = 1;
            }

            $pid = ord($buffer[0]);
            
            if($pid == UNCONNECTED_PONG::$ID){
                return true;
            }
            //--- Offline messages ----------------------------------------------------
            //Decoded and validated HERE, before getSession(): getSession() allocates a
            //Session on the very first OPEN_CONNECTION_REQUEST_1, so validating any later
            //would be too late - the object would already exist.
            //The length test comes before decode() because the readers in Packet are
            //unbounded and raise PHP warnings on a truncated buffer
            if($pid === UNCONNECTED_PING::$ID){
                if($len < UNCONNECTED_PING::$MIN_LENGTH){
                    return true;
                }
                $packet = new UNCONNECTED_PING();
                $packet->buffer = $buffer;
                $packet->decode();
                if(!$packet->isValid()){
                    return true;
                }
                //No need to create a session for just ping
                $pk = new UNCONNECTED_PONG();
                $pk->serverID = $this->serverId;
                $pk->pingID = $packet->pingID;
                $pk->serverName = $this->getName();
                $this->sendPacket($pk,$source,$port);
                return true;
            }
            if($pid === OPEN_CONNECTION_REQUEST_1::$ID || $pid === OPEN_CONNECTION_REQUEST_2::$ID){
                $packet = $this->getPacketFromPool($pid);
                if($len < $packet::$MIN_LENGTH){
                    return true;
                }
                $packet->buffer = $buffer;
                $packet->decode();
                if(!$packet->isValid()){
                    return true;
                }
                $this->handleUnconnected($packet, $source, $port);
                return true;
            }
            //--- Connected traffic ---------------------------------------------------
            if(($packet = $this->getPacketFromPool($pid)) !== null){
                if($this->sessionExists($source, $port)){
                    $packet->buffer = $buffer;
                    $this->getSession($source, $port)->handlePacket($packet);
                }
                return true;
            }

            if($buffer !== ""){
                $this->streamRaw($source, $port, $buffer);
                return true;
            }
        }

        return false;
    }

    public function sendPacket(Packet $packet, $dest, $port){
        $packet->encode();
        $this->sendBytes += $this->socket->writePacket($packet->buffer, $dest, $port);
    }

    public function streamEncapsulated(Session $session, EncapsulatedPacket $packet, $flags = RakLib::PRIORITY_NORMAL){
        $id = $session->getAddress() . ":" . $session->getPort();
        $buffer = chr(RakLib::PACKET_ENCAPSULATED) . chr(strlen($id)) . $id . chr($flags) . $packet->toBinary(true);
        $this->server->pushThreadToMainPacket($buffer);
    }

    public function streamRaw($address, $port, $payload){
        $buffer = chr(RakLib::PACKET_RAW) . chr(strlen($address)) . $address . Binary::writeShort($port) . $payload;
        $this->server->pushThreadToMainPacket($buffer);
    }

    protected function streamClose($identifier, $reason){
        $buffer = chr(RakLib::PACKET_CLOSE_SESSION) . chr(strlen($identifier)) . $identifier . chr(strlen($reason)) . $reason;
        $this->server->pushThreadToMainPacket($buffer);
    }

    protected function streamInvalid($identifier){
        $buffer = chr(RakLib::PACKET_INVALID_SESSION) . chr(strlen($identifier)) . $identifier;
        $this->server->pushThreadToMainPacket($buffer);
    }

    protected function streamOpen(Session $session){
        $identifier = $session->getAddress() . ":" . $session->getPort();
        $buffer = chr(RakLib::PACKET_OPEN_SESSION) . chr(strlen($identifier)) . $identifier . chr(strlen($session->getAddress())) . $session->getAddress() . Binary::writeShort($session->getPort()) . Binary::writeLong($session->getID());
        $this->server->pushThreadToMainPacket($buffer);
    }

    protected function streamACK($identifier, $identifierACK){
        $buffer = chr(RakLib::PACKET_ACK_NOTIFICATION) . chr(strlen($identifier)) . $identifier . Binary::writeInt($identifierACK);
        $this->server->pushThreadToMainPacket($buffer);
    }

    protected function streamOption($name, $value){
        $buffer = chr(RakLib::PACKET_SET_OPTION) . chr(strlen($name)) . $name . $value;
        $this->server->pushThreadToMainPacket($buffer);
    }

    private function checkSessions(){
        if(count($this->sessions) > 4096){
            foreach($this->sessions as $i => $s){
                if($s->isTemporal()){
                    unset($this->sessions[$i]);
                    if(count($this->sessions) <= 4096){
                        break;
                    }
                }
            }
        }
    }

    public function receiveStream(){
        $packet = $this->server->readMainToThreadPacket();
        if($packet !== null and strlen($packet) > 0){
            $id = ord($packet[0]);
            $offset = 1;
            if($id === RakLib::PACKET_ENCAPSULATED){
                $len = ord($packet[$offset++]);
                $identifier = substr($packet, $offset, $len);
                $offset += $len;
                if(isset($this->sessions[$identifier])){
                    $flags = ord($packet[$offset++]);
                    $buffer = substr($packet, $offset);
                    $this->sessions[$identifier]->addEncapsulatedToQueue(EncapsulatedPacket::fromBinary($buffer, true), $flags);
                }else{
                    $this->streamInvalid($identifier);
                }
            }elseif($id === RakLib::PACKET_RAW){
                $len = ord($packet[$offset++]);
                $address = substr($packet, $offset, $len);
                $offset += $len;
                $port = Binary::readShort(substr($packet, $offset, 2));
                $offset += 2;
                $payload = substr($packet, $offset);
                $this->socket->writePacket($payload, $address, $port);
            }elseif($id === RakLib::PACKET_CLOSE_SESSION){
                $len = ord($packet[$offset++]);
                $identifier = substr($packet, $offset, $len);
                if(isset($this->sessions[$identifier])){
                    $this->removeSession($this->sessions[$identifier]);
                }else{
                    $this->streamInvalid($identifier);
                }
            }elseif($id === RakLib::PACKET_INVALID_SESSION){
                $len = ord($packet[$offset++]);
                $identifier = substr($packet, $offset, $len);
                if(isset($this->sessions[$identifier])){
                    $this->removeSession($this->sessions[$identifier]);
                }
            }elseif($id === RakLib::PACKET_SET_OPTION){
                $len = ord($packet[$offset++]);
                $name = substr($packet, $offset, $len);
                $offset += $len;
                $value = substr($packet, $offset);
                switch($name){
                    case "name":
                        $this->name = $value;
                        break;
                    case "portChecking":
                        $this->portChecking = (bool) $value;
                        break;
                    case "packetLimit":
                        $this->packetLimit = (int) $value;
                        break;
                }
            }elseif($id === RakLib::PACKET_BLOCK_ADDRESS){
                $len = ord($packet[$offset++]);
                $address = substr($packet, $offset, $len);
                $offset += $len;
                $timeout = Binary::readInt(substr($packet, $offset, 4));
                $this->blockAddress($address, $timeout);
            }elseif($id === RakLib::PACKET_SHUTDOWN){
                foreach($this->sessions as $session){
                    $this->removeSession($session);
                }

                $this->socket->close();
                $this->shutdown = true;
            }elseif($id === RakLib::PACKET_EMERGENCY_SHUTDOWN){
                $this->shutdown = true;
            }else{
	            return false;
            }

            return true;
        }

        return false;
    }

    public function blockAddress($address, $timeout = 300){
        if(isset($this->blockExceptions[$address])){
            return;
        }

        $final = microtime(true) + $timeout;
        if(!isset($this->block[$address]) or $timeout === -1){
            if($timeout === -1){
                $final = PHP_INT_MAX;
            }else{
                $this->getLogger()->notice("Blocked $address for $timeout seconds");
            }
            $this->block[$address] = $final;
        }elseif($this->block[$address] < $final){
            $this->block[$address] = $final;
        }
    }

    /**
     * @param string $ip
     * @param int    $port
     *
     * @return Session
     */
    public function getSession($ip, $port){
        //Lookup only. Sessions are created in handleUnconnected(), once the offline
        //handshake has completed - never as a side effect of receiving a datagram.
        $id = $ip . ":" . $port;

        return isset($this->sessions[$id]) ? $this->sessions[$id] : null;
    }

    /**
     * Offline handshake. Handled here rather than in Session so that no Session object
     * exists before the exchange completes.
     */
    private function handleUnconnected(Packet $packet, $source, $port){
        $id = $source . ":" . $port;

        if($packet instanceof OPEN_CONNECTION_REQUEST_1){
            if($packet->protocol !== RakLib::PROTOCOL){
                $pk = new INCOMPATIBLE_PROTOCOL_VERSION();
                $pk->protocolVersion = RakLib::PROTOCOL;
                $pk->serverID = $this->getID();
                $this->sendPacket($pk, $source, $port);
                return;
            }

            if(!isset($this->pendingConnections[$id]) and count($this->pendingConnections) >= $this->maxPendingConnections){
                return; //pending table full: drop rather than grow without bound
            }
            $this->pendingConnections[$id] = microtime(true);

            $pk = new OPEN_CONNECTION_REPLY_1();
            $pk->mtuSize = $packet->mtuSize;
            $pk->serverID = $this->getID();
            $this->sendPacket($pk, $source, $port);
            return;
        }

        //OPEN_CONNECTION_REQUEST_2 from here on.
        if(!isset($this->pendingConnections[$id])){
            return; //no REQUEST_1 from this address: nothing to complete
        }
        if($packet->serverPort !== $this->getPort() and $this->portChecking){
            return;
        }
        if(isset($this->sessions[$id])){
            return; //already connected: never rebuild a session under a live one
        }

        //A GUID is not a credential: it travels in clear in every handshake, so anyone
        //who can read one could otherwise drop the session it belongs to from any address.
        //Only accept the takeover from the SAME address - a client rebinding its source
        //port. From another address, refuse without touching the existing session.
        $existing = $this->getSessionByClientID($packet->clientID);
        if($existing !== null){
            if($existing->getAddress() !== $source){
                $this->getLogger()->notice("Refused GUID takeover of " . $existing->getAddress() . ":" . $existing->getPort() . " by " . $id);
                return;
            }
            $this->removeSession($existing, "Guid reused by new connection");
        }

        //Clamp both ends: RakNet minimum MTU is 576. A value of 0 (or anything < 34) would
        //make str_split() length negative in addEncapsulatedToQueue() and crash the thread.
        $mtuSize = min(max((int) abs($packet->mtuSize), 576), 1464);

        unset($this->pendingConnections[$id]);
        $this->checkSessions();
        $session = new Session($this, $source, $port);
        $session->acceptConnection($packet->clientID, $mtuSize);
        $this->sessions[$id] = $session;

        $pk = new OPEN_CONNECTION_REPLY_2();
        $pk->mtuSize = $mtuSize;
        $pk->serverID = $this->getID();
        $pk->clientAddress = $source;
        $pk->clientPort = $port;
        $this->sendPacket($pk, $source, $port);
    }

    public function removeSession(Session $session, $reason = "unknown"){
        $id = $session->getAddress() . ":" . $session->getPort();
        if(isset($this->sessions[$id])){
            $this->sessions[$id]->close();
            unset($this->sessions[$id]);
            $this->streamClose($id, $reason);
        }
    }

    public function openSession(Session $session){
        $this->streamOpen($session);
    }

    public function notifyACK(Session $session, $identifierACK){
        $this->streamACK($session->getAddress() . ":" . $session->getPort(), $identifierACK);
    }

    public function getName(){
        return $this->name;
    }

    public function getID(){
        return $this->serverId;
    }

	private function registerPacket($id, $class){
		$this->packetPool[$id] = new $class;
	}

	/**
	 * @param $id
	 *
	 * @return Packet
	 */
	public function getPacketFromPool($id){
		if(isset($this->packetPool[$id])){
			return clone $this->packetPool[$id];
		}

		return null;
	}

    private function registerPackets(){
        //$this->registerPacket(UNCONNECTED_PING::$ID, UNCONNECTED_PING::class);
        $this->registerPacket(UNCONNECTED_PING_OPEN_CONNECTIONS::$ID, UNCONNECTED_PING_OPEN_CONNECTIONS::class);
        $this->registerPacket(OPEN_CONNECTION_REQUEST_1::$ID, OPEN_CONNECTION_REQUEST_1::class);
        $this->registerPacket(OPEN_CONNECTION_REPLY_1::$ID, OPEN_CONNECTION_REPLY_1::class);
        $this->registerPacket(OPEN_CONNECTION_REQUEST_2::$ID, OPEN_CONNECTION_REQUEST_2::class);
        $this->registerPacket(OPEN_CONNECTION_REPLY_2::$ID, OPEN_CONNECTION_REPLY_2::class);
        $this->registerPacket(UNCONNECTED_PONG::$ID, UNCONNECTED_PONG::class);
        $this->registerPacket(ADVERTISE_SYSTEM::$ID, ADVERTISE_SYSTEM::class);
        $this->registerPacket(DATA_PACKET_0::$ID, DATA_PACKET_0::class);
        $this->registerPacket(DATA_PACKET_1::$ID, DATA_PACKET_1::class);
        $this->registerPacket(DATA_PACKET_2::$ID, DATA_PACKET_2::class);
        $this->registerPacket(DATA_PACKET_3::$ID, DATA_PACKET_3::class);
        $this->registerPacket(DATA_PACKET_4::$ID, DATA_PACKET_4::class);
        $this->registerPacket(DATA_PACKET_5::$ID, DATA_PACKET_5::class);
        $this->registerPacket(DATA_PACKET_6::$ID, DATA_PACKET_6::class);
        $this->registerPacket(DATA_PACKET_7::$ID, DATA_PACKET_7::class);
        $this->registerPacket(DATA_PACKET_8::$ID, DATA_PACKET_8::class);
        $this->registerPacket(DATA_PACKET_9::$ID, DATA_PACKET_9::class);
        $this->registerPacket(DATA_PACKET_A::$ID, DATA_PACKET_A::class);
        $this->registerPacket(DATA_PACKET_B::$ID, DATA_PACKET_B::class);
        $this->registerPacket(DATA_PACKET_C::$ID, DATA_PACKET_C::class);
        $this->registerPacket(DATA_PACKET_D::$ID, DATA_PACKET_D::class);
        $this->registerPacket(DATA_PACKET_E::$ID, DATA_PACKET_E::class);
        $this->registerPacket(DATA_PACKET_F::$ID, DATA_PACKET_F::class);
        $this->registerPacket(NACK::$ID, NACK::class);
        $this->registerPacket(ACK::$ID, ACK::class);
        //Same acknowledgement, but carrying the B and AS flag (0x20) that our own 0x84
        //datagrams request. Without this entry it fell through to streamRaw() and the
        //recovery queue was never released.
        $this->registerPacket(0xe0, ACK::class);
    }
}
