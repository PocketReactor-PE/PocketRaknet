<?php

namespace pocketraknet\protocol;

use pocketraknet\RakLib;

class INCOMPATIBLE_PROTOCOL_VERSION extends OfflineMessage {

    public static $ID = 0x19;
    public static $MIN_LENGTH = 26; //1 (ID) + 1 (protocol) + 16 (MAGIC) + 8 (serverID)

    public $protocolVersion;
    public $serverID;

    public function encode()
    {
        parent::encode();
        $this->putByte($this->protocolVersion);
        $this->writeMagic();
        $this->putLong($this->serverID);
    }

    public function decode(){
        parent::decode();
        $this->protocolVersion = $this->getByte();
        $this->readMagic();
        $this->serverID = $this->getLong();
    }
}