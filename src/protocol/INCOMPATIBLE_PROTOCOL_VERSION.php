<?php

namespace raklib\protocol;

use raklib\RakLib;

class INCOMPATIBLE_PROTOCOL_VERSION extends Packet {

    public static $ID = 0x19;

    public $protocolVersion;
    public $serverID;

    public function encode()
    {
        parent::encode();
        $this->putByte($this->protocolVersion);
        $this->put(RakLib::MAGIC);
        $this->putLong($this->serverID);
    }

    public function decode(){
        parent::decode();
        $this->protocolVersion = $this->getByte();
        $this->offset += 16; //MAGIC
        $this->serverID = $this->getLong();
    }
}