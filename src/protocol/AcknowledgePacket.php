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

namespace pocketraknet\protocol;

use pocketraknet\Binary;


abstract class AcknowledgePacket extends Packet{
    /** Ceiling on datagram numbers decoded from one packet, all ranges together. */
    const MAX_ENTRIES = 4096;

    /** @var int[] */
    public $packets = [];

    public function encode(){
        parent::encode();
        $payload = "";
        sort($this->packets, SORT_NUMERIC);
        $count = count($this->packets);
        $records = 0;

        if($count > 0){
            $pointer = 1;
            $start = $this->packets[0];
            $last = $this->packets[0];

            while($pointer < $count){
                $current = $this->packets[$pointer++];
                $diff = $current - $last;
                if($diff === 1){
                    $last = $current;
                }elseif($diff > 1){ //Forget about duplicated packets (bad queues?)
                    if($start === $last){
                        $payload .= "\x01";
                        $payload .= Binary::writeLTriad($start);
                        $start = $last = $current;
                    }else{
                        $payload .= "\x00";
                        $payload .= Binary::writeLTriad($start);
                        $payload .= Binary::writeLTriad($last);
                        $start = $last = $current;
                    }
                    ++$records;
                }
            }

            if($start === $last){
                $payload .= "\x01";
                $payload .= Binary::writeLTriad($start);
            }else{
                $payload .= "\x00";
                $payload .= Binary::writeLTriad($start);
                $payload .= Binary::writeLTriad($last);
            }
            ++$records;
        }

        $this->putShort($records);
        $this->buffer .= $payload;
    }

    public function decode(){
        parent::decode();
        //Datagram flags: 0x40 = acknowledgement, 0x20 = carries B and AS WHEN 0x40 is set.
        //In that case DatagramHeaderFormat::Serialize slips four arrival-rate bytes between
        //the flags and the range list. The mask covers BOTH bits: on a NAK (0xa0) the 0x20
        //IS the NAK flag and no float precedes the ranges.
        if((ord($this->buffer[0]) & 0x60) === 0x60){
            $this->offset += 4;
        }
        $count = $this->getShort();
        $this->packets = [];
        $cnt = 0;
        //The binary's RangeList has no per-range cap: a NAK after a burst loss legitimately
        //covers up to 1000 datagrams (the hole cap of CCRakNetSlidingWindow::OnGotPacket).
        //Only the total is bounded, to keep a forged list from expanding without limit.
        for($i = 0; $i < $count and !$this->feof() and $cnt < self::MAX_ENTRIES; ++$i){
            if($this->getByte() === 0){
                $start = $this->getLTriad();
                $end = $this->getLTriad();
                if(($end - $start) > (self::MAX_ENTRIES - $cnt)){
                    $end = $start + (self::MAX_ENTRIES - $cnt);
                }
                for($c = $start; $c <= $end; ++$c){
                    $this->packets[$cnt++] = $c;
                }
            }else{
                $this->packets[$cnt++] = $this->getLTriad();
            }
        }
    }

	public function clean(){
		$this->packets = [];
		return parent::clean();
	}
}