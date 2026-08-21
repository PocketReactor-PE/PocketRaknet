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

#include <rules/RakLibPacket.h>

class PONG_DataPacket extends Packet
{
    public static $ID = 0x03;

    public $pingID;
    public $pongID;

    public function encode()
    {
        parent::encode();
        $this->putLong($this->pingID);
        //The client requires exactly 17 bytes: 0x03 + its timestamp + ours. A 9-byte pong is
        //rejected by Update and dropped before it ever reaches the game layer.
        $this->putLong($this->pongID);
    }

    public function decode()
    {
        parent::decode();
        $this->pingID = $this->getLong();
        $this->pongID = $this->getLong();
    }
}
