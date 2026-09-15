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

use pocketraknet\protocol\ACK;
use pocketraknet\protocol\CLIENT_CONNECT_DataPacket;
use pocketraknet\protocol\CLIENT_DISCONNECT_DataPacket;
use pocketraknet\protocol\CLIENT_HANDSHAKE_DataPacket;
use pocketraknet\protocol\DATA_PACKET_0;
use pocketraknet\protocol\DATA_PACKET_4;
use pocketraknet\protocol\DataPacket;
use pocketraknet\protocol\EncapsulatedPacket;
use pocketraknet\protocol\NACK;
use pocketraknet\protocol\Packet;
use pocketraknet\protocol\PING_DataPacket;
use pocketraknet\protocol\PONG_DataPacket;
use pocketraknet\protocol\SERVER_HANDSHAKE_DataPacket;
use pocketraknet\RakLib;

/**
 * One connected peer.
 *
 * The transport rules below are those of the RakNet build embedded in MCPE 0.15.10, read from
 * its decompilation (ReliabilityLayer, CCRakNetSlidingWindow, RangeList). Where this class
 * used to follow the old RakLib heuristics instead, the differences were real: every counter
 * is 24 bits on the wire and wraps, a late datagram must be re-acknowledged, ordering is per
 * channel and keyed on the order index (not on the message number), and the resend delay
 * follows the measured round trip.
 */
class Session{
    const STATE_UNCONNECTED = 0;
    const STATE_CONNECTING_1 = 1;
    const STATE_CONNECTING_2 = 2;
    const STATE_CONNECTED = 3;

    const MAX_SPLIT_SIZE = 128;
    const MAX_SPLIT_COUNT = 4;

    /** Every datagram number, message number, order index and sequence index is 24 bits. */
    const INDEX_MASK = 0xffffff;

    /** There are exactly 32 ordering channels; ReliabilityLayer::Send folds anything above onto 0. */
    const CHANNEL_COUNT = 32;

    /**
     * CCRakNetSlidingWindow::OnGotPacket: a datagram more than 1000 ahead of the expected one
     * only gets 1000 holes negatively acknowledged, and one more than 50000 ahead is rejected.
     */
    const MAX_DATAGRAM_HOLES = 1000;
    const MAX_DATAGRAM_JUMP = 50000;

    /**
     * Resend delay, in seconds. CCRakNetSlidingWindow::GetRTOForRetransmission returns 2s
     * until a round trip has been measured, then 2*rtt + 4*deviation + 30ms, capped at 2s.
     * The smoothing factor 0.05 is the literal read in OnAck.
     */
    const DEFAULT_RTO = 2.0;
    const RTO_MARGIN = 0.03;
    const RTT_ALPHA = 0.05;

    /** RakPeer::Update schedules a connected ping every 5 seconds. */
    const PING_INTERVAL = 5;

    /** Peer inactivity timeout, in seconds; the peer's own reliability layer uses 10s. */
    const TIMEOUT = 10;

    /** How many queued datagrams are flushed per tick. */
    const SEND_PER_TICK = 16;

    /** @var SessionManager|null */
    private $sessionManager;
    private $address;
    private $port;
    private $state = self::STATE_UNCONNECTED;
    private $mtuSize = 548; //Min size
    private $id = 0;

    private $lastUpdate;
    private $startTime;
    private $isActive;
    private $isTemporal = true;

    //----- Send side ---------------------------------------------------------------------
    private $messageIndex = 0;
    private $sendSeqNumber = 0;
    private $splitID = 0;
    /** @var int[] next order index per channel (mOrderedWriteIndex) */
    private $orderedWriteIndex = [];
    /** @var int[] next sequence index per channel (mSequencedWriteIndex) */
    private $sequencedWriteIndex = [];

    /** @var DataPacket[] datagrams waiting for their turn on the wire */
    private $packetToSend = [];
    /** @var DataPacket[] datagrams sent and not yet acknowledged, keyed by datagram number */
    private $recoveryQueue = [];
    /** @var DataPacket the datagram being filled */
    private $sendQueue;
    /** @var int[][] */
    private $needACK = [];

    /** Round-trip estimate in seconds, -1 until the first measurement. */
    private $rttEstimated = -1.0;
    private $rttDeviation = 0.0;
    private $lastPingTime = 0.0;
    private $latency = -1;

    //----- Receive side ------------------------------------------------------------------
    /** Next datagram number expected (CCRakNetSlidingWindow::expectedNextSequenceNumber). */
    private $expectedSeqNumber = 0;
    /** @var int[] */
    private $ACKQueue = [];
    /** @var int[] */
    private $NACKQueue = [];

    /** Next message number expected; anything below it (modulo 2^24) is a duplicate. */
    private $reliableBase = 0;
    /** @var true[] message numbers received ahead of $reliableBase, keyed by number */
    private $reliableReceived = [];

    /** @var int[] order index expected per channel (mOrderedReadIndex) */
    private $orderedReadIndex = [];
    /** @var int[] (mHighestSequencedReadIndex) */
    private $highestSequencedReadIndex = [];
    /** @var array[] per channel, per order index: ["s" => [sequenceIndex => packet], "o" => packet] */
    private $orderingHeld = [];

    /** @var EncapsulatedPacket[][] */
    private $splitPackets = [];
    /** @var int[] splitCount announced by the FIRST fragment of each splitID */
    private $splitCounts = [];

    public function __construct(SessionManager $sessionManager, $address, $port){
        $this->sessionManager = $sessionManager;
        $this->address = $address;
        $this->port = $port;
        $this->sendQueue = new DATA_PACKET_4();
        $this->lastUpdate = microtime(true);
        $this->startTime = microtime(true);
        $this->lastPingTime = $this->startTime;
        $this->isActive = false;

        for($i = 0; $i < self::CHANNEL_COUNT; ++$i){
            $this->orderedWriteIndex[$i] = 0;
            $this->sequencedWriteIndex[$i] = 0;
            $this->orderedReadIndex[$i] = 0;
            $this->highestSequencedReadIndex[$i] = 0;
            $this->orderingHeld[$i] = [];
        }
    }

    public function getAddress(){
        return $this->address;
    }

    public function getPort(){
        return $this->port;
    }

    public function getID(){
        return $this->id;
    }

    public function getState(){
        return $this->state;
    }

    public function isTemporal(){
        return $this->isTemporal;
    }

    /** Last measured round trip to the peer in milliseconds, -1 before the first pong. */
    public function getLatency(){
        return $this->latency;
    }

    //=====================================================================================
    // 24-bit helpers
    //=====================================================================================

    private static function nextIndex($index){
        return ($index + 1) & self::INDEX_MASK;
    }

    /**
     * ReliabilityLayer::IsOlderOrderedPacket, branch for branch, 0x800002 included. It is an
     * artefact of RakNet's maxRange/2 arithmetic on a 24-bit integer, but it does move the
     * boundary by two units in the upper half of the space, and simplifying it to 0x800000
     * would change the answer for two values out of sixteen million.
     * Returns true when $newIndex is BEHIND $waitingFor, i.e. stale.
     */
    public static function isOlderOrderedPacket($newIndex, $waitingFor){
        if($waitingFor > 0x7fffff){
            if($newIndex < $waitingFor){
                $bound = ($waitingFor + 0x800002) & self::INDEX_MASK;
                return !($newIndex < $bound);
            }
            return false;
        }
        if($waitingFor <= $newIndex){
            $bound = ($waitingFor + 0x800000) & self::INDEX_MASK;
            if($newIndex < $bound){
                return false;
            }
        }
        return true;
    }

    //=====================================================================================
    // Tick
    //=====================================================================================

    public function update($time){
        if(!$this->isActive and ($this->lastUpdate + self::TIMEOUT) < $time){
            $this->disconnect("timeout");

            return;
        }
        $this->isActive = false;

        if(count($this->ACKQueue) > 0){
            $pk = new ACK();
            $pk->packets = $this->ACKQueue;
            $this->sendPacket($pk);
            $this->ACKQueue = [];
        }

        if(count($this->NACKQueue) > 0){
            $pk = new NACK();
            $pk->packets = $this->NACKQueue;
            $this->sendPacket($pk);
            $this->NACKQueue = [];
        }

        if(count($this->packetToSend) > 0){
            $limit = self::SEND_PER_TICK;
            foreach($this->packetToSend as $k => $pk){
                $pk->sendTime = $time;
                $pk->encode();
                $this->recoveryQueue[$pk->seqNumber] = $pk;
                unset($this->packetToSend[$k]);
                $this->sendPacket($pk);

                if(--$limit <= 0){
                    break;
                }
            }
            //The backlog is NEVER truncated. It only holds reliable datagrams awaiting a
            //(re)send: dropping one would leave a hole the peer's ordered channel waits on
            //forever, and the peer cannot ask for it again since it never saw it. The queue
            //is bounded by what the peer acknowledges; a peer that stops acknowledging
            //times out above.
        }

        if(count($this->needACK) > 0){
            foreach($this->needACK as $identifierACK => $indexes){
                if(count($indexes) === 0){
                    unset($this->needACK[$identifierACK]);
                    $this->sessionManager->notifyACK($this, $identifierACK);
                }
            }
        }

        $limite = $time - $this->getRTO();
        foreach($this->recoveryQueue as $seq => $pk){
            if($pk->sendTime < $limite){
                //A resend keeps its MESSAGE numbers but takes a NEW datagram number, exactly as
                //the peer does. Reusing the old one would make it count as a duplicate datagram
                //and corrupt the peer's round-trip measurement.
                $pk->seqNumber = $this->sendSeqNumber;
                $this->sendSeqNumber = self::nextIndex($this->sendSeqNumber);
                $this->packetToSend[] = $pk;
                unset($this->recoveryQueue[$seq]);
            }else{
                //Insertion order is send order, so the first datagram still inside its
                //delay means every later one is too.
                break;
            }
        }

        if($this->state === self::STATE_CONNECTED and ($time - $this->lastPingTime) >= self::PING_INTERVAL){
            $this->lastPingTime = $time;
            $this->sendPing();
        }

        $this->sendQueue();
    }

    public function disconnect($reason = "unknown"){
        $this->sessionManager->removeSession($this, $reason);
    }

    private function sendPacket(Packet $packet){
        $this->sessionManager->sendPacket($packet, $this->address, $this->port);
    }

    //=====================================================================================
    // Round trip
    //=====================================================================================

    private function onRttSample($rtt){
        if($rtt < 0){
            return;
        }
        if($this->rttEstimated < 0){
            //First measurement: taken as-is, no smoothing.
            $this->rttEstimated = $rtt;
            $this->rttDeviation = $rtt;
            return;
        }
        $delta = $rtt - $this->rttEstimated;
        $this->rttEstimated += $delta * self::RTT_ALPHA;
        $this->rttDeviation += (abs($delta) - $this->rttDeviation) * self::RTT_ALPHA;
    }

    /** Resend delay in seconds, see DEFAULT_RTO. */
    public function getRTO(){
        if($this->rttEstimated < 0){
            return self::DEFAULT_RTO;
        }
        $rto = $this->rttEstimated * 2 + $this->rttDeviation * 4 + self::RTO_MARGIN;
        return $rto > self::DEFAULT_RTO ? self::DEFAULT_RTO : $rto;
    }

    /** Our clock in milliseconds since the session opened: the RakNet::GetTime() the peer expects. */
    private function clockMillis(){
        return (int) ((microtime(true) - $this->startTime) * 1000);
    }

    private function sendPing(){
        $pk = new PING_DataPacket();
        $pk->pingID = $this->clockMillis();
        $pk->encode();

        $sendPacket = new EncapsulatedPacket();
        $sendPacket->reliability = 0;
        $sendPacket->buffer = $pk->buffer;
        $this->addToQueue($sendPacket);
    }

    //=====================================================================================
    // Send side
    //=====================================================================================

    public function sendQueue(){
        if(count($this->sendQueue->packets) > 0){
            $this->sendQueue->seqNumber = $this->sendSeqNumber;
            $this->sendSeqNumber = self::nextIndex($this->sendSeqNumber);
            $this->sendPacket($this->sendQueue);
            $this->sendQueue->sendTime = microtime(true);
            $this->recoveryQueue[$this->sendQueue->seqNumber] = $this->sendQueue;
            $this->sendQueue = new DATA_PACKET_4();
        }
    }

    /**
     * @param EncapsulatedPacket $pk
     * @param int                $flags
     */
    private function addToQueue(EncapsulatedPacket $pk, $flags = RakLib::PRIORITY_NORMAL){
        $priority = $flags & 0b0000111;
        if($pk->needACK and $pk->messageIndex !== null){
            $this->needACK[$pk->identifierACK][$pk->messageIndex] = $pk->messageIndex;
        }
        if($priority === RakLib::PRIORITY_IMMEDIATE){ //Skip queues
            $packet = new DATA_PACKET_0();
            $packet->seqNumber = $this->sendSeqNumber;
            $this->sendSeqNumber = self::nextIndex($this->sendSeqNumber);
            if($pk->needACK){
                $packet->packets[] = clone $pk;
                $pk->needACK = false;
            }else{
                $packet->packets[] = $pk->toBinary();
            }

            $this->sendPacket($packet);
            $packet->sendTime = microtime(true);
            $this->recoveryQueue[$packet->seqNumber] = $packet;

            return;
        }
        $length = $this->sendQueue->length();
        if($length + $pk->getTotalLength() > $this->mtuSize){
            $this->sendQueue();
        }

        if($pk->needACK){
            $this->sendQueue->packets[] = clone $pk;
            $pk->needACK = false;
        }else{
            $this->sendQueue->packets[] = $pk->toBinary();
        }
    }

    /**
     * Largest payload that fits in one fragment. ReliabilityLayer::SplitPacket uses
     * MTU - 9 - 23 (datagram reserve + largest message header); we keep two bytes more.
     */
    private function getSplitChunkSize(){
        return $this->mtuSize - 34;
    }

    /**
     * ReliabilityLayer::Send. Folds the inputs like the binary does (it never refuses, it
     * replaces), assigns the counters, and splits what does not fit in one datagram.
     *
     * @param EncapsulatedPacket $packet
     * @param int                $flags
     */
    public function addEncapsulatedToQueue(EncapsulatedPacket $packet, $flags = RakLib::PRIORITY_NORMAL){

        if(($packet->needACK = ($flags & RakLib::FLAG_NEED_ACK) > 0) === true){
            $this->needACK[$packet->identifierACK] = [];
        }

        //The ACK-receipt variants only exist inside RakLib ($needACK carries the request);
        //on the wire, and for every counter below, they are their plain counterpart.
        $reliability = EncapsulatedPacket::wireReliability((int) $packet->reliability);
        if($reliability < 0 or $reliability > 7){
            $reliability = 2;
        }
        $channel = (int) $packet->orderChannel;
        if($channel < 0 or $channel >= self::CHANNEL_COUNT){
            $channel = 0;
        }

        $chunkSize = $this->getSplitChunkSize();
        if($chunkSize < 1){
            //Defensive guard: str_split() with length <= 0 throws a ValueError on PHP 8+
            //and would take the whole RakLib thread down. A malformed MTU should never reach here
            //(clamped at negotiation), but never trust it.
            return;
        }
        $needsSplit = strlen($packet->buffer) > $chunkSize;
        if($needsSplit){
            //A message too big for one block is FORCED reliable: a lost fragment would
            //otherwise leave the rest of the message stuck forever in the peer's reassembler.
            if($reliability === 0){
                $reliability = 2;
            }elseif($reliability === 1){
                $reliability = 4;
            }
        }
        $packet->reliability = $reliability;

        if($reliability >= 2){
            $packet->messageIndex = $this->messageIndex;
            $this->messageIndex = self::nextIndex($this->messageIndex);
        }

        //The asymmetry at the heart of the mechanism: a SEQUENCED message reads the order
        //index without advancing it (it stays in the current batch), while an ORDERED message
        //advances it AND resets the channel's sequence counter (it opens a new batch).
        if($reliability === 1 or $reliability === 4){
            $packet->orderChannel = $channel;
            $packet->orderIndex = $this->orderedWriteIndex[$channel];
            $packet->sequenceIndex = $this->sequencedWriteIndex[$channel];
            $this->sequencedWriteIndex[$channel] = self::nextIndex($this->sequencedWriteIndex[$channel]);
        }elseif($reliability === 3){
            $packet->orderChannel = $channel;
            $packet->orderIndex = $this->orderedWriteIndex[$channel];
            $this->orderedWriteIndex[$channel] = self::nextIndex($this->orderedWriteIndex[$channel]);
            $this->sequencedWriteIndex[$channel] = 0;
        }

        if(!$needsSplit){
            $this->addToQueue($packet, $flags);
            return;
        }

        $buffers = str_split($packet->buffer, $chunkSize);
        $splitID = $this->splitID;
        $this->splitID = ($this->splitID + 1) & 0xffff;
        $count = count($buffers);
        foreach($buffers as $index => $buffer){
            $pk = new EncapsulatedPacket();
            $pk->splitID = $splitID;
            $pk->hasSplit = true;
            $pk->splitCount = $count;
            $pk->splitIndex = $index;
            $pk->reliability = $reliability;
            $pk->buffer = $buffer;
            //Every fragment carries its own message number; the ordering fields are those
            //of the whole message.
            if($index > 0){
                $pk->messageIndex = $this->messageIndex;
                $this->messageIndex = self::nextIndex($this->messageIndex);
            }else{
                $pk->messageIndex = $packet->messageIndex;
            }
            $pk->orderChannel = $packet->orderChannel;
            $pk->orderIndex = $packet->orderIndex;
            $pk->sequenceIndex = $packet->sequenceIndex;
            $pk->needACK = $packet->needACK;
            $pk->identifierACK = $packet->identifierACK;
            $this->addToQueue($pk, $flags | RakLib::PRIORITY_IMMEDIATE);
        }
    }

    //=====================================================================================
    // Receive side
    //=====================================================================================

    public function handlePacket(Packet $packet){
        $this->isActive = true;
        $this->lastUpdate = microtime(true);
        if($this->state !== self::STATE_CONNECTED and $this->state !== self::STATE_CONNECTING_2){
            return;
        }

        if($packet::$ID >= 0x80 and $packet::$ID <= 0x8f and $packet instanceof DataPacket){ //Data packet
            $packet->decode();
            if(!$this->acceptDatagram($packet->seqNumber)){
                return;
            }
            foreach($packet->packets as $pk){
                $this->handleEncapsulatedPacket($pk);
            }
        }elseif($packet instanceof ACK){
            $packet->decode();
            $now = microtime(true);
            foreach($packet->packets as $seq){
                if(isset($this->recoveryQueue[$seq])){
                    $datagram = $this->recoveryQueue[$seq];
                    //The round trip is measured on the datagram: that is what gets acknowledged.
                    //A resend carries a new datagram number, so an old acknowledgement can never
                    //be mistaken for the new send.
                    $this->onRttSample($now - $datagram->sendTime);
                    foreach($datagram->packets as $pk){
                        if($pk instanceof EncapsulatedPacket and $pk->needACK and $pk->messageIndex !== null){
                            unset($this->needACK[$pk->identifierACK][$pk->messageIndex]);
                        }
                    }
                    unset($this->recoveryQueue[$seq]);
                }
            }
        }elseif($packet instanceof NACK){
            $packet->decode();
            foreach($packet->packets as $seq){
                if(isset($this->recoveryQueue[$seq])){
                    $pk = $this->recoveryQueue[$seq];
                    $pk->seqNumber = $this->sendSeqNumber;
                    $this->sendSeqNumber = self::nextIndex($this->sendSeqNumber);
                    $this->packetToSend[] = $pk;
                    unset($this->recoveryQueue[$seq]);
                }
            }
        }
    }

    /**
     * CCRakNetSlidingWindow::OnGotPacket. Decides whether a datagram is processed, always
     * acknowledges what is accepted, and negatively acknowledges the holes it reveals.
     *
     *   expected == n         -> accepted, no hole, expected advances
     *   n BEHIND expected     -> accepted WITHOUT advancing: the peer lost our ACK and needs
     *                            it again. Duplicate messages inside are caught by number.
     *   n ahead               -> holes = n - expected (capped at 1000; > 50000 is rejected)
     */
    private function acceptDatagram($seq){
        $seq &= self::INDEX_MASK;
        if($seq === $this->expectedSeqNumber){
            $this->expectedSeqNumber = self::nextIndex($seq);
        }elseif(((($this->expectedSeqNumber - $seq) & self::INDEX_MASK) & 0x800000) === 0){
            //Late or duplicated: re-acknowledge, keep expected where it is.
        }else{
            $gap = ($seq - $this->expectedSeqNumber) & self::INDEX_MASK;
            if($gap > self::MAX_DATAGRAM_JUMP){
                return false;
            }
            if($gap > self::MAX_DATAGRAM_HOLES){
                $gap = self::MAX_DATAGRAM_HOLES;
            }
            for($i = $gap; $i > 0; --$i){
                $hole = ($seq - $i) & self::INDEX_MASK;
                $this->NACKQueue[$hole] = $hole;
            }
            $this->expectedSeqNumber = self::nextIndex($seq);
        }
        unset($this->NACKQueue[$seq]);
        $this->ACKQueue[$seq] = $seq;
        return true;
    }

    /**
     * The hasReceivedPacketQueue of ReliabilityLayer: true when this message number was
     * already processed. Only messages that carry a number on the wire come here.
     */
    private function isDuplicateMessage($messageIndex){
        $messageIndex &= self::INDEX_MASK;
        $holeCount = ($messageIndex - $this->reliableBase) & self::INDEX_MASK;
        if($holeCount === 0){
            //Exactly the one we were waiting for: advance past everything already received.
            $this->reliableBase = self::nextIndex($this->reliableBase);
            while(isset($this->reliableReceived[$this->reliableBase])){
                unset($this->reliableReceived[$this->reliableBase]);
                $this->reliableBase = self::nextIndex($this->reliableBase);
            }
            return false;
        }
        if($holeCount > 0x7fffff){
            return true; //older than the base: the lower half of the circular space
        }
        if(isset($this->reliableReceived[$messageIndex])){
            return true;
        }
        //Memory is bounded by what the peer actually sends, not by the distance announced.
        $this->reliableReceived[$messageIndex] = true;
        return false;
    }

    /**
     * Same order as HandleSocketReceiveFromConnectedPlayer: duplicates, fragments, then the
     * ordering channel. A replayed reliable message is never delivered twice, and a
     * RELIABLE_ORDERED ahead of its turn waits for its predecessor.
     */
    private function handleEncapsulatedPacket(EncapsulatedPacket $packet){
        if($packet->messageIndex !== null and $this->isDuplicateMessage($packet->messageIndex)){
            return;
        }

        if($packet->hasSplit){
            if($this->state !== self::STATE_CONNECTED){
                return;
            }
            $packet = $this->handleSplit($packet);
            if($packet === null){
                return;
            }
        }

        //Only reliabilities 1, 3 and 4 go through the ordering machinery (mask 0x1a, < 5).
        $reliability = $packet->reliability;
        if($reliability === 1 or $reliability === 3 or $reliability === 4){
            $this->handleOrdered($packet);
        }else{
            $this->handleEncapsulatedPacketRoute($packet);
        }
    }

    /**
     * @return EncapsulatedPacket|null the whole message once its last fragment arrived
     */
    private function handleSplit(EncapsulatedPacket $packet){
        if($packet->splitCount >= self::MAX_SPLIT_SIZE or $packet->splitIndex >= self::MAX_SPLIT_SIZE or $packet->splitIndex < 0){
            return null;
        }

        if(!isset($this->splitPackets[$packet->splitID])){
            if(count($this->splitPackets) >= self::MAX_SPLIT_COUNT){
                return null;
            }
            $this->splitPackets[$packet->splitID] = [$packet->splitIndex => $packet];
            $this->splitCounts[$packet->splitID] = $packet->splitCount;
        }else{
            //The splitCount is pinned by the first fragment. A fragment announcing a different
            //one belongs to another message: accepting it would let count() match a smaller
            //splitCount while indexes are still missing, and the reassembly loop would read
            //absent slots (PHP warnings + a corrupt reassembled message).
            if($this->splitCounts[$packet->splitID] !== $packet->splitCount){
                return null;
            }
            //A replayed fragment must neither overwrite nor count twice.
            if(isset($this->splitPackets[$packet->splitID][$packet->splitIndex])){
                return null;
            }
            $this->splitPackets[$packet->splitID][$packet->splitIndex] = $packet;
        }
        if(count($this->splitPackets[$packet->splitID]) !== $this->splitCounts[$packet->splitID]){
            return null;
        }

        $pk = new EncapsulatedPacket();
        $pk->buffer = "";
        for($i = 0; $i < $this->splitCounts[$packet->splitID]; ++$i){
            $pk->buffer .= $this->splitPackets[$packet->splitID][$i]->buffer;
        }
        $pk->length = strlen($pk->buffer);
        //The ordering fields are those of the message, identical on every fragment.
        $pk->reliability = $packet->reliability;
        $pk->orderChannel = $packet->orderChannel;
        $pk->orderIndex = $packet->orderIndex;
        $pk->sequenceIndex = $packet->sequenceIndex;
        unset($this->splitPackets[$packet->splitID], $this->splitCounts[$packet->splitID]);

        return $pk;
    }

    /**
     * The ordering part of HandleSocketReceiveFromConnectedPlayer, for reliabilities 1, 3, 4.
     */
    private function handleOrdered(EncapsulatedPacket $packet){
        $channel = (int) $packet->orderChannel;
        if($channel < 0 or $channel >= self::CHANNEL_COUNT){
            $channel = 0;
        }
        $orderIndex = ((int) $packet->orderIndex) & self::INDEX_MASK;
        $sequenced = ($packet->reliability === 1 or $packet->reliability === 4);

        if($orderIndex === $this->orderedReadIndex[$channel]){
            if($sequenced){
                //In the current batch. A sequenced message is only delivered if it is NEWER
                //than the last one delivered: stragglers are dropped, that is the point of
                //the sequenced mode (a stale player position is useless).
                $sequenceIndex = ((int) $packet->sequenceIndex) & self::INDEX_MASK;
                if(self::isOlderOrderedPacket($sequenceIndex, $this->highestSequencedReadIndex[$channel])){
                    return;
                }
                $this->highestSequencedReadIndex[$channel] = self::nextIndex($sequenceIndex);
                $this->handleEncapsulatedPacketRoute($packet);
                return;
            }
            //Ordered and expected: deliver, advance, open a new batch, then drain everything
            //its arrival unblocks.
            $this->handleEncapsulatedPacketRoute($packet);
            $this->orderedReadIndex[$channel] = self::nextIndex($orderIndex);
            $this->highestSequencedReadIndex[$channel] = 0;
            $this->drainOrdered($channel);
            return;
        }

        //Not the expected index: either ahead, and we keep it, or behind, and it was already
        //delivered.
        if(self::isOlderOrderedPacket($orderIndex, $this->orderedReadIndex[$channel])){
            return;
        }
        if($sequenced){
            $this->orderingHeld[$channel][$orderIndex]["s"][((int) $packet->sequenceIndex) & self::INDEX_MASK] = $packet;
        }else{
            $this->orderingHeld[$channel][$orderIndex]["o"] = $packet;
        }
    }

    /**
     * Delivers what the held queue has for the expected order index, batch after batch, until
     * a batch has no ordered message to close it. Within a batch the sequenced messages come
     * first (lowest sequence index first), the ordered one last: that is the emission order.
     *
     * One deliberate departure from the binary: it leaves mHighestSequencedReadIndex at the
     * raw sequence index (no +1) and does not reset it when the drained ordered message opens
     * the next batch, so the next batch's first sequenced message can be dropped as stale.
     * Here the drained path does exactly what the direct path does.
     */
    private function drainOrdered($channel){
        while(isset($this->orderingHeld[$channel][$this->orderedReadIndex[$channel]])){
            $orderIndex = $this->orderedReadIndex[$channel];
            $slot = $this->orderingHeld[$channel][$orderIndex];
            unset($this->orderingHeld[$channel][$orderIndex]);

            if(isset($slot["s"])){
                ksort($slot["s"], SORT_NUMERIC);
                foreach($slot["s"] as $sequenceIndex => $pk){
                    if(self::isOlderOrderedPacket($sequenceIndex, $this->highestSequencedReadIndex[$channel])){
                        continue;
                    }
                    $this->highestSequencedReadIndex[$channel] = self::nextIndex($sequenceIndex);
                    $this->handleEncapsulatedPacketRoute($pk);
                }
            }
            if(!isset($slot["o"])){
                break;
            }
            $this->handleEncapsulatedPacketRoute($slot["o"]);
            $this->orderedReadIndex[$channel] = self::nextIndex($orderIndex);
            $this->highestSequencedReadIndex[$channel] = 0;
        }
    }

    private function handleEncapsulatedPacketRoute(EncapsulatedPacket $packet){
        if($this->sessionManager === null){
            return;
        }
        if($packet->buffer === "" or $packet->buffer === null){
            return;
        }

        $id = ord($packet->buffer[0]);
        if($id < 0x80){ //internal data packet
            if($this->state === self::STATE_CONNECTING_2){
                if($id === CLIENT_CONNECT_DataPacket::$ID){
                    $dataPacket = new CLIENT_CONNECT_DataPacket;
                    $dataPacket->buffer = $packet->buffer;
                    $dataPacket->decode();
                    $pk = new SERVER_HANDSHAKE_DataPacket;
                    $pk->address = $this->address;
                    $pk->port = $this->port;
                    $pk->sendPing = $dataPacket->sendPing;
                    //Observed on the real 0.15.10 host: the second timestamp is the first plus
                    //exactly 1000. Plain 64-bit arithmetic; bcmath was never needed here.
                    $pk->sendPong = $dataPacket->sendPing + 1000;
                    $pk->encode();

                    $sendPacket = new EncapsulatedPacket();
                    $sendPacket->reliability = 0;
                    $sendPacket->buffer = $pk->buffer;
                    $this->addToQueue($sendPacket, RakLib::PRIORITY_IMMEDIATE);
                }elseif($id === CLIENT_HANDSHAKE_DataPacket::$ID){
                    $dataPacket = new CLIENT_HANDSHAKE_DataPacket;
                    $dataPacket->buffer = $packet->buffer;
                    $dataPacket->decode();

                    if($dataPacket->port === $this->sessionManager->getPort() or !$this->sessionManager->portChecking){
                        $this->state = self::STATE_CONNECTED; //FINALLY!
                        $this->isTemporal = false;
                        $this->lastPingTime = microtime(true);
                        $this->sessionManager->openSession($this);
                    }
                }
            }elseif($id === CLIENT_DISCONNECT_DataPacket::$ID){
                $this->disconnect("client disconnect");
            }elseif($id === PING_DataPacket::$ID){
                $dataPacket = new PING_DataPacket;
                $dataPacket->buffer = $packet->buffer;
                $dataPacket->decode();

                $pk = new PONG_DataPacket;
                $pk->pingID = $dataPacket->pingID;
                //Our clock, in milliseconds since the session opened: the RakNet::GetTime() value
                //the peer expects as the second field of the 17 bytes.
                $pk->pongID = $this->clockMillis();
                $pk->encode();

                $sendPacket = new EncapsulatedPacket();
                $sendPacket->reliability = 0;
                $sendPacket->buffer = $pk->buffer;
                $this->addToQueue($sendPacket);
            }elseif($id === PONG_DataPacket::$ID){
                //Answer to our own ping: the first field is the clock we sent.
                if(strlen($packet->buffer) === 17){
                    $dataPacket = new PONG_DataPacket;
                    $dataPacket->buffer = $packet->buffer;
                    $dataPacket->decode();
                    $latency = $this->clockMillis() - $dataPacket->pingID;
                    if($latency >= 0){
                        $this->latency = $latency;
                    }
                }
            }
        }elseif($this->state === self::STATE_CONNECTED){
            $this->sessionManager->streamEncapsulated($this, $packet);
        }else{
            //$this->sessionManager->getLogger()->notice("Received packet before connection: " . bin2hex($packet->buffer));
        }
    }

    /**
     * Called by SessionManager once the offline handshake has completed. The session only
     * exists from that point on, so it starts already past REQUEST_2 and never has to
     * handle an offline packet itself.
     */
    public function acceptConnection($clientId, $mtuSize){
        $this->id = $clientId;
        $this->mtuSize = $mtuSize;
        $this->state = self::STATE_CONNECTING_2;
    }

    public function close(){
        $data = "\x00\x00\x08\x15";
        $this->addEncapsulatedToQueue(EncapsulatedPacket::fromBinary($data), RakLib::PRIORITY_IMMEDIATE); //CLIENT_DISCONNECT packet 0x15
        $this->sessionManager = null;
    }
}
