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

use pocketraknet\RakLib;

class UNCONNECTED_PING extends OfflineMessage{
    public static $ID = 0x01;
    public static $MIN_LENGTH = 25; //1 (ID) + 8 (pingID) + 16 (MAGIC)

    public $pingID;

    public function encode(){
        parent::encode();
        $this->putLong($this->pingID);
        $this->writeMagic();
    }

    public function decode(){
        parent::decode();
        $this->pingID = $this->getLong();
        $this->readMagic();
    }
}