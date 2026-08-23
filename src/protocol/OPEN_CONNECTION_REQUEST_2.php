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

class OPEN_CONNECTION_REQUEST_2 extends OfflineMessage{
    public static $ID = 0x07;

    public static $MIN_LENGTH = 34; //1 + 16 + 7 (IPv4 address) + 2 (mtu) + 8 (clientID)

    public $clientID;
	public $serverAddress;
    public $serverPort;
    public $mtuSize;

    public function encode(){
        parent::encode();
        $this->writeMagic();
		$this->putAddress($this->serverAddress, $this->serverPort, 4);
        $this->putShort($this->mtuSize);
        $this->putLong($this->clientID);
    }

    public function decode(){
        parent::decode();
        $this->readMagic();
		$this->getAddress($this->serverAddress, $this->serverPort);
        $this->mtuSize = $this->getShort();
        $this->clientID = $this->getLong();
    }
}
