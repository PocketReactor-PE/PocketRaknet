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

namespace pocketraknet\protocol;

use pocketraknet\Binary;

class EncapsulatedPacket{

    public $reliability;
    public $hasSplit = false;
    public $length = 0;
    public $messageIndex = null;
    public $orderIndex = null;
    public $sequenceIndex = null;
    public $orderChannel = null;
    public $splitCount = null;
    public $splitID = null;
    public $splitIndex = null;
    public $buffer;
    public $needACK = false;
    public $identifierACK = null;

    /**
     * @param string $binary
     * @param bool   $internal
     * @param int    &$offset
     *
     * @return EncapsulatedPacket
     */
    public static function fromBinary($binary, $internal = false, &$offset = null){

	    $packet = new EncapsulatedPacket();
        //Real buffer length. Every field is checked against it BEFORE being read, and above
        //all before the $offset increment it triggers. Otherwise substr() returns "" and
        //Binary::readLTriad() emits two PHP warnings per truncated header.
        $len = strlen($binary);
        //Smallest possible header: 1 flags byte + 2 length bytes. The binary refuses outright
        //below 4 bytes (0x20 bits); the internal variant needs 9 (1 + 4 + 4).
        $minimum = $internal ? 9 : 4;
        if($len < $minimum){
            $offset = $len;
            $packet->buffer = "";
            return $packet;
        }
        $flags = ord($binary[0]);
        $packet->reliability = $reliability = ($flags & 0b11100000) >> 5;
        $packet->hasSplit = $hasSplit = ($flags & 0b00010000) > 0;
        if($internal){
            $length = Binary::readInt(substr($binary, 1, 4));
            $packet->identifierACK = Binary::readInt(substr($binary, 5, 4));
            $offset = 9;
        }else{
            $length = (int) ceil(Binary::readShort(substr($binary, 1, 2)) / 8);
            $offset = 3;
	        $packet->identifierACK = null;
        }


        /*
         * From http://www.jenkinssoftware.com/raknet/manual/reliabilitytypes.html
         *
         * Default: 0b010 (2) or 0b011 (3)
         *
         * 0: UNRELIABLE
         * 1: UNRELIABLE_SEQUENCED
         * 2: RELIABLE
         * 3: RELIABLE_ORDERED
         * 4: RELIABLE_SEQUENCED
         * 5: UNRELIABLE_WITH_ACK_RECEIPT
         * 6: RELIABLE_WITH_ACK_RECEIPT
         * 7: RELIABLE_ORDERED_WITH_ACK_RECEIPT
         */

		if($reliability >=2 && $reliability !== 5){
            if($offset + 3 > $len){
                $offset = $len;
                $packet->buffer = "";
                return $packet;
            }
            $packet->messageIndex = Binary::readLTriad(substr($binary, $offset, 3));
            $offset += 3;
        }
        if($reliability === 1 || $reliability === 4){
            if($offset + 3 > $len){
                $offset = $len;
                $packet->buffer = "";
                return $packet;
            }
            $packet->sequenceIndex = Binary::readLTriad(substr($binary, $offset, 3));
            $offset += 3;
        }
        if($reliability === 1 || $reliability === 3 || $reliability === 4 || $reliability === 7){
            if($offset + 4 > $len){
                $offset = $len;
                $packet->buffer = "";
                return $packet;
            }
            $packet->orderIndex = Binary::readLTriad(substr($binary, $offset, 3));
            $offset += 3;
            $packet->orderChannel = ord($binary[$offset++]);
        }

        if($hasSplit){
            if($offset + 10 > $len){
                $offset = $len;
                $packet->buffer = "";
                return $packet;
            }
            $packet->splitCount = Binary::readInt(substr($binary, $offset, 4));
            $offset += 4;
            $packet->splitID = Binary::readShort(substr($binary, $offset, 2));
            $offset += 2;
            $packet->splitIndex = Binary::readInt(substr($binary, $offset, 4));
            $offset += 4;
        }
        if($length <= 0 || ($packet->orderChannel !== null && $packet->orderChannel >= 32)){
            $packet->buffer = "";
            return $packet;
        }
        if($hasSplit && $packet->splitIndex >= $packet->splitCount){
            $packet->buffer = "";
            return $packet;
        }
        //Last check: the announced payload must actually fit in the buffer. Otherwise
        //substr() would return a truncated one and $offset would run past $len.
        if($offset + $length > $len){
            $offset = $len;
            $packet->buffer = "";
            return $packet;
        }
        $packet->buffer = substr($binary, $offset, $length);
        $offset += $length;

        return $packet;
    }

    public function getTotalLength()
    {
        //The same tests as toBinary(), and for the same reason: toBinary() writes these fields
        //based on the RELIABILITY, not on whether the property is null. Counting on "!== null"
        //under-reported reliabilities 1 and 4 by 3 bytes, where the sequenceIndex is always
        //written (through "?? 0") even when it was never assigned.
        return 3 + strlen($this->buffer)
            + (($this->reliability >= 2 && $this->reliability !== 5) ? 3 : 0)
            + (($this->reliability === 1 || $this->reliability === 4) ? 3 : 0)
            + (($this->reliability === 1 || $this->reliability === 3 || $this->reliability === 4 || $this->reliability === 7) ? 4 : 0)
            + ($this->hasSplit ? 10 : 0);
    }


        /**
     * @param bool $internal
     *
     * @return string
     */
    public function toBinary($internal = false){
        return
			chr(($this->reliability << 5) | ($this->hasSplit ? 0b00010000 : 0)) .
			($internal ? Binary::writeInt(strlen($this->buffer)) . Binary::writeInt($this->identifierACK) : Binary::writeShort(strlen($this->buffer) << 3)) .
            (($this->reliability >= 2 && $this->reliability !== 5) ? Binary::writeLTriad($this->messageIndex) : "") .
            (($this->reliability === 1 || $this->reliability === 4) ? Binary::writeLTriad($this->sequenceIndex ?? 0) : "") .
            (($this->reliability === 1 || $this->reliability === 3 || $this->reliability === 4 || $this->reliability === 7)
                    ? Binary::writeLTriad($this->orderIndex) . chr($this->orderChannel) : "") .
			($this->hasSplit ? Binary::writeInt($this->splitCount) . Binary::writeShort($this->splitID) . Binary::writeInt($this->splitIndex) : "")
			. $this->buffer;
    }

    public function __toString(){
        return $this->toBinary();
    }
}
